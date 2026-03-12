use axum::{extract::Query, routing::get, Router};
use memmap2::Mmap;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::sync::Arc;

// --- S2 helpers ---

const STREET_CELL_LEVEL: u64 = 15;
const ADMIN_CELL_LEVEL: u64 = 10;

fn cell_id_at_level(lat: f64, lng: f64, level: u64) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level).0
}

fn cell_neighbors_at_level(cell_id: u64, level: u64) -> Vec<u64> {
    let cell = CellID(cell_id);
    cell.all_neighbors(level).into_iter().map(|c| c.0).collect()
}

// --- Binary format structs (must match C++ build pipeline) ---

#[repr(C)]
#[derive(Clone, Copy)]
struct WayHeader {
    node_offset: u32,
    node_count: u8,
    name_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AddrPoint {
    lat: f32,
    lng: f32,
    housenumber_id: u32,
    street_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct InterpWay {
    node_offset: u32,
    node_count: u8,
    street_id: u32,
    start_number: u32,
    end_number: u32,
    interpolation: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AdminPolygon {
    vertex_offset: u32,
    vertex_count: u16,
    name_id: u32,
    admin_level: u8,
    area: f32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeCoord {
    lat: f32,
    lng: f32,
}

// --- Index data ---

struct Index {
    street_cells: Mmap,
    street_entries: Mmap,
    street_ways: Mmap,
    street_nodes: Mmap,
    addr_cells: Mmap,
    addr_entries: Mmap,
    addr_points: Mmap,
    interp_cells: Mmap,
    interp_entries: Mmap,
    interp_ways: Mmap,
    interp_nodes: Mmap,
    admin_cells: Mmap,
    admin_entries: Mmap,
    admin_polygons: Mmap,
    admin_vertices: Mmap,
    strings: Mmap,
}

fn mmap_file(path: &str) -> Mmap {
    let file = File::open(path).unwrap_or_else(|e| panic!("Failed to open {}: {}", path, e));
    unsafe { Mmap::map(&file).unwrap_or_else(|e| panic!("Failed to mmap {}: {}", path, e)) }
}

impl Index {
    fn load(dir: &str) -> Self {
        Index {
            street_cells: mmap_file(&format!("{}/street_cells.bin", dir)),
            street_entries: mmap_file(&format!("{}/street_entries.bin", dir)),
            street_ways: mmap_file(&format!("{}/street_ways.bin", dir)),
            street_nodes: mmap_file(&format!("{}/street_nodes.bin", dir)),
            addr_cells: mmap_file(&format!("{}/addr_cells.bin", dir)),
            addr_entries: mmap_file(&format!("{}/addr_entries.bin", dir)),
            addr_points: mmap_file(&format!("{}/addr_points.bin", dir)),
            interp_cells: mmap_file(&format!("{}/interp_cells.bin", dir)),
            interp_entries: mmap_file(&format!("{}/interp_entries.bin", dir)),
            interp_ways: mmap_file(&format!("{}/interp_ways.bin", dir)),
            interp_nodes: mmap_file(&format!("{}/interp_nodes.bin", dir)),
            admin_cells: mmap_file(&format!("{}/admin_cells.bin", dir)),
            admin_entries: mmap_file(&format!("{}/admin_entries.bin", dir)),
            admin_polygons: mmap_file(&format!("{}/admin_polygons.bin", dir)),
            admin_vertices: mmap_file(&format!("{}/admin_vertices.bin", dir)),
            strings: mmap_file(&format!("{}/strings.bin", dir)),
        }
    }

    fn get_string(&self, offset: u32) -> &str {
        let bytes = &self.strings[offset as usize..];
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[..end]).unwrap_or("")
    }

    fn read_u16(data: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    }

    fn read_u32(data: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
    }

    fn read_u64(data: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    // Binary search cell index: each entry is 12 bytes (u64 cell_id + u32 offset)
    fn lookup_cell_ids(cells: &[u8], entries: &[u8], cell_id: u64) -> Vec<u32> {
        let entry_size: usize = 12;
        let count = cells.len() / entry_size;
        if count == 0 { return vec![]; }

        let mut lo = 0usize;
        let mut hi = count;
        let mut found = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = Self::read_u64(cells, mid * entry_size);
            if mid_id == cell_id {
                found = Some(mid);
                break;
            } else if mid_id < cell_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        let idx = match found {
            Some(i) => i,
            None => return vec![],
        };

        let offset = Self::read_u32(cells, idx * entry_size + 8) as usize;
        if offset + 2 > entries.len() { return vec![]; }

        let id_count = Self::read_u16(entries, offset) as usize;
        let data_start = offset + 2;
        let data_end = data_start + id_count * 4;
        if data_end > entries.len() { return vec![]; }

        (0..id_count)
            .map(|i| Self::read_u32(entries, data_start + i * 4))
            .collect()
    }

    // --- Address lookup ---

    fn find_nearest_addr(&self, lat: f64, lng: f64) -> Option<(f64, &AddrPoint)> {
        let cell = cell_id_at_level(lat, lng, STREET_CELL_LEVEL);
        let neighbors = cell_neighbors_at_level(cell, STREET_CELL_LEVEL);

        let all_points: &[AddrPoint] = unsafe {
            std::slice::from_raw_parts(
                self.addr_points.as_ptr() as *const AddrPoint,
                self.addr_points.len() / std::mem::size_of::<AddrPoint>(),
            )
        };

        let mut best_dist = f64::MAX;
        let mut best_point: Option<&AddrPoint> = None;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let ids = Self::lookup_cell_ids(&self.addr_cells, &self.addr_entries, c);
            for id in ids {
                let point = &all_points[id as usize];
                let dist = haversine_approx(lat, lng, point.lat as f64, point.lng as f64);
                if dist < best_dist {
                    best_dist = dist;
                    best_point = Some(point);
                }
            }
        }

        best_point.map(|p| (best_dist, p))
    }

    // --- Street lookup ---

    fn find_nearest_street(&self, lat: f64, lng: f64) -> Option<(f64, &WayHeader)> {
        let cell = cell_id_at_level(lat, lng, STREET_CELL_LEVEL);
        let neighbors = cell_neighbors_at_level(cell, STREET_CELL_LEVEL);

        let all_ways: &[WayHeader] = unsafe {
            std::slice::from_raw_parts(
                self.street_ways.as_ptr() as *const WayHeader,
                self.street_ways.len() / std::mem::size_of::<WayHeader>(),
            )
        };
        let all_nodes: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.street_nodes.as_ptr() as *const NodeCoord,
                self.street_nodes.len() / std::mem::size_of::<NodeCoord>(),
            )
        };

        let mut best_dist = f64::MAX;
        let mut best_way: Option<&WayHeader> = None;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let ids = Self::lookup_cell_ids(&self.street_cells, &self.street_entries, c);
            for id in ids {
                let way = &all_ways[id as usize];
                let offset = way.node_offset as usize;
                let count = way.node_count as usize;
                let nodes = &all_nodes[offset..offset + count];

                for i in 0..nodes.len() - 1 {
                    let dist = point_to_segment_distance(
                        lat, lng,
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                    );
                    if dist < best_dist {
                        best_dist = dist;
                        best_way = Some(way);
                    }
                }
            }
        }

        best_way.map(|w| (best_dist, w))
    }

    // --- Interpolation lookup ---

    fn find_nearest_interp(&self, lat: f64, lng: f64) -> Option<(f64, String, u32)> {
        let cell = cell_id_at_level(lat, lng, STREET_CELL_LEVEL);
        let neighbors = cell_neighbors_at_level(cell, STREET_CELL_LEVEL);

        let all_interps: &[InterpWay] = unsafe {
            std::slice::from_raw_parts(
                self.interp_ways.as_ptr() as *const InterpWay,
                self.interp_ways.len() / std::mem::size_of::<InterpWay>(),
            )
        };
        let all_nodes: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.interp_nodes.as_ptr() as *const NodeCoord,
                self.interp_nodes.len() / std::mem::size_of::<NodeCoord>(),
            )
        };

        if all_interps.is_empty() { return None; }

        let mut best_dist = f64::MAX;
        let mut best_interp: Option<&InterpWay> = None;
        let mut best_t: f64 = 0.0;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let ids = Self::lookup_cell_ids(&self.interp_cells, &self.interp_entries, c);
            for id in ids {
                let iw = &all_interps[id as usize];
                if iw.start_number == 0 || iw.end_number == 0 { continue; }

                let offset = iw.node_offset as usize;
                let count = iw.node_count as usize;
                let nodes = &all_nodes[offset..offset + count];

                let mut total_len: f64 = 0.0;
                for i in 0..nodes.len() - 1 {
                    total_len += haversine_approx(
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                    );
                }
                if total_len == 0.0 { continue; }

                let mut best_seg_dist = f64::MAX;
                let mut best_seg_t: f64 = 0.0;
                let mut prev_accumulated: f64 = 0.0;

                for i in 0..nodes.len() - 1 {
                    let seg_len = haversine_approx(
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                    );
                    let (dist, seg_t) = point_to_segment_with_t(
                        lat, lng,
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                    );
                    if dist < best_seg_dist {
                        best_seg_dist = dist;
                        best_seg_t = (prev_accumulated + seg_t * seg_len) / total_len;
                    }
                    prev_accumulated += seg_len;
                }

                if best_seg_dist < best_dist {
                    best_dist = best_seg_dist;
                    best_interp = Some(iw);
                    best_t = best_seg_t;
                }
            }
        }

        best_interp.map(|iw| {
            let start = iw.start_number as f64;
            let end = iw.end_number as f64;
            let raw = start + best_t * (end - start);

            let step: u32 = match iw.interpolation {
                1 | 2 => 2,
                _ => 1,
            };

            let number = if step == 2 {
                let base = iw.start_number;
                let offset = ((raw - base as f64) / step as f64).round() as u32 * step;
                base + offset
            } else {
                raw.round() as u32
            };

            let street = self.get_string(iw.street_id).to_string();
            (best_dist, street, number)
        })
    }

    // --- Admin boundary lookup (point-in-polygon) ---

    fn find_admin(&self, lat: f64, lng: f64) -> AdminResult {
        let cell = cell_id_at_level(lat, lng, ADMIN_CELL_LEVEL);
        let neighbors = cell_neighbors_at_level(cell, ADMIN_CELL_LEVEL);

        let all_polygons: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(
                self.admin_polygons.as_ptr() as *const AdminPolygon,
                self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
            )
        };
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };

        // For each admin level, find the smallest-area polygon containing the point
        let mut best_by_level: [Option<(f32, &AdminPolygon)>; 12] = [None; 12];

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let ids = Self::lookup_cell_ids(&self.admin_cells, &self.admin_entries, c);
            for id in ids {
                let poly = &all_polygons[id as usize];
                let level = poly.admin_level as usize;
                if level >= 12 { continue; }

                // Skip if we already have a smaller polygon at this level
                if let Some((best_area, _)) = best_by_level[level] {
                    if poly.area >= best_area { continue; }
                }

                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                let vertices = &all_vertices[offset..offset + count];

                if point_in_polygon(lat as f32, lng as f32, vertices) {
                    best_by_level[level] = Some((poly.area, poly));
                }
            }
        }

        let mut result = AdminResult::default();

        for level in 0..12 {
            if let Some((_, poly)) = best_by_level[level] {
                let name = self.get_string(poly.name_id).to_string();
                match poly.admin_level {
                    2 => result.country = Some(name),
                    4 => result.state = Some(name),
                    6 => result.county = Some(name),
                    8 => result.city = Some(name),
                    11 => result.postcode = Some(name),
                    _ => {}
                }
            }
        }

        result
    }

    // --- Combined query ---

    fn query(&self, lat: f64, lng: f64) -> Address {
        let max_addr_dist = 0.0005; // ~50m
        let max_street_dist = 0.005; // ~500m

        let admin = self.find_admin(lat, lng);

        // 1. Try nearest address point
        if let Some((dist, point)) = self.find_nearest_addr(lat, lng) {
            if dist < max_addr_dist {
                return Address {
                    housenumber: Some(self.get_string(point.housenumber_id).to_string()),
                    street: Some(self.get_string(point.street_id).to_string()),
                    city: admin.city,
                    state: admin.state,
                    postcode: admin.postcode,
                    country: admin.country,
                };
            }
        }

        // 2. Try interpolation
        if let Some((dist, street, number)) = self.find_nearest_interp(lat, lng) {
            if dist < max_addr_dist {
                return Address {
                    housenumber: Some(number.to_string()),
                    street: Some(street),
                    city: admin.city,
                    state: admin.state,
                    postcode: admin.postcode,
                    country: admin.country,
                };
            }
        }

        // 3. Fall back to nearest street
        if let Some((dist, way)) = self.find_nearest_street(lat, lng) {
            if dist < max_street_dist {
                return Address {
                    housenumber: None,
                    street: Some(self.get_string(way.name_id).to_string()),
                    city: admin.city,
                    state: admin.state,
                    postcode: admin.postcode,
                    country: admin.country,
                };
            }
        }

        // 4. Admin only
        if admin.country.is_some() || admin.city.is_some() {
            return Address {
                housenumber: None,
                street: None,
                city: admin.city,
                state: admin.state,
                postcode: admin.postcode,
                country: admin.country,
            };
        }

        Address::default()
    }
}

// --- Geometry helpers ---

fn haversine_approx(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let cos_lat = ((lat1 + lat2) / 2.0).to_radians().cos();
    (dlat * dlat + dlng * dlng * cos_lat * cos_lat).sqrt()
}

fn point_to_segment_with_t(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
) -> (f64, f64) {
    let dx = bx - ax;
    let dy = by - ay;
    let len_sq = dx * dx + dy * dy;

    let t = if len_sq == 0.0 {
        0.0
    } else {
        (((px - ax) * dx + (py - ay) * dy) / len_sq).clamp(0.0, 1.0)
    };

    let proj_x = ax + t * dx;
    let proj_y = ay + t * dy;
    (haversine_approx(px, py, proj_x, proj_y), t)
}

fn point_to_segment_distance(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
) -> f64 {
    point_to_segment_with_t(px, py, ax, ay, bx, by).0
}

// Ray casting point-in-polygon test
fn point_in_polygon(lat: f32, lng: f32, vertices: &[NodeCoord]) -> bool {
    let mut inside = false;
    let n = vertices.len();
    let mut j = n - 1;

    for i in 0..n {
        let vi = &vertices[i];
        let vj = &vertices[j];

        if ((vi.lng > lng) != (vj.lng > lng))
            && (lat < (vj.lat - vi.lat) * (lng - vi.lng) / (vj.lng - vi.lng) + vi.lat)
        {
            inside = !inside;
        }
        j = i;
    }

    inside
}

// --- API types ---

#[derive(Default)]
struct AdminResult {
    country: Option<String>,
    state: Option<String>,
    county: Option<String>,
    city: Option<String>,
    postcode: Option<String>,
}

#[derive(Serialize, Default)]
struct Address {
    #[serde(skip_serializing_if = "Option::is_none")]
    housenumber: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
}

#[derive(Deserialize)]
struct QueryParams {
    lat: f64,
    lng: f64,
}

async fn reverse_geocode(
    Query(params): Query<QueryParams>,
    index: axum::extract::State<Arc<Index>>,
) -> axum::Json<Address> {
    axum::Json(index.query(params.lat, params.lng))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args.get(1).map(|s| s.as_str()).unwrap_or(".");
    let bind_addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3000");

    eprintln!("Loading index from {}...", data_dir);
    let index = Arc::new(Index::load(data_dir));
    eprintln!("Index loaded. Starting server on {}...", bind_addr);

    let app = Router::new()
        .route("/reverse", get(reverse_geocode))
        .with_state(index);

    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
