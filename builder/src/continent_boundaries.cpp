#include "continent_boundaries.h"

// Simplified continent polygons — rough boundaries that follow major
// geographic features (coastlines, mountain ranges, isthmuses).
// These separate geocoding data more accurately than simple bboxes
// while being cheap to test (point-in-polygon with ~20-50 vertices each).

std::vector<ContinentPolygon> get_continent_polygons() {
    std::vector<ContinentPolygon> polys;

    // Order MUST match kContinents[] in continent_filter.cpp:
    // africa, asia, europe, north-america, south-america, oceania, central-america, antarctica

    // Africa: south of Mediterranean, west of Suez
    polys.push_back({"africa", {
        {-35.0, -25.0},  // SW Atlantic
        {-35.0, 55.0},   // SE Indian Ocean
        {12.5, 43.0},    // Horn of Africa
        {29.0, 32.5},    // Suez
        {32.0, 34.5},    // Sinai
        {37.0, 12.0},    // Tunisia
        {36.0, 0.0},     // Algeria coast
        {36.0, -5.5},    // Gibraltar
        {35.0, -25.0},   // Morocco/Atlantic
        {-35.0, -25.0},  // close
    }});

    // Asia: east of Europe/Urals, east of Suez, north of Australia
    polys.push_back({"asia", {
        {-12.0, 95.0},   // Indonesia south
        {-12.0, 180.0},  // Pacific
        {82.0, 180.0},   // Arctic east
        {82.0, 60.0},    // Arctic/Ural
        {67.0, 66.0},    // Ural north
        {60.0, 60.0},    // Ural
        {55.0, 59.0},    // Ural middle
        {50.0, 52.0},    // Ural south
        {47.0, 42.0},    // Caspian
        {45.0, 40.5},    // Caucasus
        {43.0, 40.0},    // Black Sea east
        {42.0, 29.5},    // Bosphorus
        {41.0, 29.0},    // Istanbul
        {36.5, 36.0},    // Syria/Turkey
        {32.0, 34.5},    // Israel/Sinai
        {29.0, 32.5},    // Suez
        {12.5, 43.0},    // Horn of Africa/Yemen
        {-12.0, 95.0},   // close via Indian Ocean
    }});

    // Europe: roughly bounded by Ural Mountains (60°E), Mediterranean,
    // Atlantic, and Arctic. Excludes Turkey (Asia) but includes
    // European Russia west of Urals.
    polys.push_back({"europe", {
        {35.0, -25.0},  // SW: Atlantic/Morocco
        {36.0, -5.5},   // Gibraltar
        {36.0, 0.0},    // Mediterranean
        {35.5, 12.0},   // Tunisia coast
        {35.0, 25.0},   // Crete
        {36.5, 28.0},   // Rhodes
        {39.0, 26.5},   // Aegean
        {41.0, 29.0},   // Istanbul/Bosphorus
        {42.0, 29.5},   // Black Sea west
        {43.0, 40.0},   // Caucasus
        {45.0, 40.5},   // North Caucasus
        {47.0, 42.0},   // Caspian west
        {50.0, 52.0},   // Ural south
        {55.0, 59.0},   // Ural middle
        {60.0, 60.0},   // Ural north
        {67.0, 66.0},   // Ural/Arctic
        {72.0, 60.0},   // Novaya Zemlya
        {72.0, -25.0},  // Arctic/Iceland
        {35.0, -25.0},  // close
    }});

    // North America: north of Panama, includes Caribbean
    polys.push_back({"north-america", {
        {7.0, -170.0},   // Pacific/Panama
        {84.0, -170.0},  // Arctic west
        {84.0, -50.0},   // Arctic east/Greenland
        {60.0, -42.0},   // Greenland south
        {45.0, -50.0},   // Atlantic
        {25.0, -60.0},   // Caribbean
        {7.0, -77.0},    // Panama
        {7.0, -170.0},   // close
    }});

    // South America
    polys.push_back({"south-america", {
        {-56.0, -82.0},  // Patagonia
        {13.0, -82.0},   // Colombia/Caribbean
        {13.0, -34.0},   // NE Atlantic
        {-56.0, -34.0},  // SE Atlantic
        {-56.0, -82.0},  // close
    }});

    // Oceania: Australia, NZ, Pacific Islands
    polys.push_back({"oceania", {
        {-50.0, 110.0},  // SW
        {-50.0, 180.0},  // SE
        {0.0, 180.0},    // NE Pacific
        {0.0, 110.0},    // NW
        {-50.0, 110.0},  // close
    }});

    // Central America + Caribbean (overlap zone between N/S America)
    polys.push_back({"central-america", {
        {7.0, -120.0},   // Pacific
        {23.5, -120.0},  // Mexico north
        {23.5, -57.0},   // Caribbean east
        {7.0, -57.0},    // Venezuela
        {7.0, -120.0},   // close
    }});

    // Antarctica
    polys.push_back({"antarctica", {
        {-90.0, -180.0},
        {-90.0, 180.0},
        {-60.0, 180.0},
        {-60.0, -180.0},
        {-90.0, -180.0},
    }});

    return polys;
}
