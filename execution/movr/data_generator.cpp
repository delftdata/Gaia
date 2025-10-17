#include "execution/movr/data_generator.h"

#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <iomanip>

#include "common/proto_utils.h"
#include "common/string_utils.h"
#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"
#include "workload/workload.h"

using std::bernoulli_distribution;
using std::iota;
using std::string;
using std::stringstream;
using std::to_string;
using std::unordered_set;
using std::vector;

namespace slog {
namespace movr {

std::string DataGenerator::GenerateRevenue(std::mt19937& rng) {
    std::uniform_real_distribution<double> dist(1.0, 100.0);
    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << dist(rng);
    return EnsureFixedLength<64>(ss.str());
}

std::string DataGenerator::GenerateRandomVehicleType(std::mt19937& rng) {
    static const std::vector<std::string> types = {"skateboard", "bike", "scooter"};
    std::uniform_int_distribution<> dist(0, types.size() - 1);
    return EnsureFixedLength<64>(types[dist(rng)]);
}

std::string DataGenerator::GetVehicleAvailability(std::mt19937& rng) {
    static const std::vector<std::pair<std::string, double>> choices = {
        {"available", 0.4}, {"in_use", 0.55}, {"lost", 0.05}
    };
    return EnsureFixedLength<64>(WeightedChoice(rng, choices));
}

std::string DataGenerator::GenerateRandomColor(std::mt19937& rng) {
    static const std::vector<std::string> colors = {"red", "yellow", "blue", "green", "black"};
    std::uniform_int_distribution<> dist(0, colors.size() - 1);
    return EnsureFixedLength<64>(colors[dist(rng)]);
}

std::pair<std::string, std::string> DataGenerator::GenerateRandomLatLong(std::mt19937& rng) {
    std::uniform_real_distribution<double> lat_dist(-90.0, 90.0);
    std::uniform_real_distribution<double> lon_dist(-180.0, 180.0);
    std::stringstream ss_lat, ss_lon;
    ss_lat << std::fixed << std::setprecision(6) << lat_dist(rng);
    ss_lon << std::fixed << std::setprecision(6) << lon_dist(rng);
    return {EnsureFixedLength<64>(ss_lat.str()), EnsureFixedLength<64>(ss_lon.str())};
}

std::string DataGenerator::GenerateBikeBrand(std::mt19937& rng) {
    static const std::vector<std::string> brands = {
        "Merida", "Fuji", "Cervelo", "Pinarello", "Santa Cruz", "Kona", "Schwinn"
    };
    std::uniform_int_distribution<> dist(0, brands.size() - 1);
    return EnsureFixedLength<64>(brands[dist(rng)]);
}

std::string DataGenerator::GenerateVehicleMetadata(std::mt19937& rng, const std::string& type) {
    std::string color = GenerateRandomColor(rng);
    std::string brand = (type == "bike") ? GenerateBikeBrand(rng) : "";

    // Simple JSON-like string
    std::string result = "{\"color\": \"" + color + "\"";
    if (!brand.empty()) {
        result += ", \"brand\": \"" + brand + "\"";
    }
    result += "}";
    return EnsureFixedLength<64>(result);
}

std::string DataGenerator::GenerateName(std::mt19937& rng) {
    static const std::vector<std::string> first_names = {
        "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", 
        "Thomas", "Charles", "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", 
        "Barbara", "Susan", "Jessica", "Sarah", "Karen"
    };

    static const std::vector<std::string> last_names = {
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", 
        "Garcia", "Rodriguez", "Wilson", "Martinez", "Anderson", "Taylor", 
        "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White"
    };

    std::uniform_int_distribution<size_t> first_dist(0, first_names.size() - 1);
    std::uniform_int_distribution<size_t> last_dist(0, last_names.size() - 1);

    std::string result = first_names[first_dist(rng)] + " " + last_names[last_dist(rng)];
    return EnsureFixedLength<64>(result);
}

std::string DataGenerator::GenerateAddress(std::mt19937& rng) {
    static const std::vector<std::string> street_names = {
        "Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "View",
        "Washington", "Lake", "Hill", "Park", "Sunset", "Highland",
        "Railroad", "Church", "Willow", "Meadow", "Broad", "Forest", "River"};

    static const std::vector<std::string> street_suffixes = {
        "St", "Ave", "Blvd", "Rd", "Ln", "Dr", "Ct", "Pl", "Cir", "Way"};

    std::uniform_int_distribution<int> house_num_dist(100, 9999);
    std::uniform_int_distribution<size_t> street_dist(0, street_names.size() - 1);
    std::uniform_int_distribution<size_t> suffix_dist(0, street_suffixes.size() - 1);

    std::string result = std::to_string(house_num_dist(rng)) + " " + street_names[street_dist(rng)] + " " + street_suffixes[suffix_dist(rng)];
    return EnsureFixedLength<64>(result);
}

std::string DataGenerator::GenerateCreditCard(std::mt19937& rng) {
    std::string number;
    std::uniform_int_distribution<int> digit_dist(0, 9);

    for (int i = 0; i < 16; i++) {
        if (i > 0 && i % 4 == 0) {
            number += " ";
        }
        number += std::to_string(digit_dist(rng));
    }
    return EnsureFixedLength<64>(number);
}

std::string DataGenerator::GeneratePromoCode(std::mt19937& rng) {
    static const std::vector<std::string> words = {
        "free", "summer", "discount", "save", "code", "quick",
        "deal", "offer", "special", "limited", "bonus", "credit",
        "voucher", "gift", "reward", "holiday", "welcome", "newuser"
    };
    std::uniform_int_distribution<size_t> word_dist(0, words.size() - 1);
    std::string result = words[word_dist(rng)] + "_" + words[word_dist(rng)] + "_" + words[word_dist(rng)];
    return EnsureFixedLength<64>(result);
}

std::string DataGenerator::GenerateDescription(std::mt19937& rng) {
    std::string result = "This is a description";
    return EnsureFixedLength<64>(result);
}

std::string DataGenerator::GenerateRules(std::mt19937& rng) {
    std::string result = "This is a list of rules";
    return EnsureFixedLength<64>(result);
}

// Function to generate a potentially contended ID based on contention_factor_
std::string DataGenerator::GenerateContendedID(std::mt19937& rng, double contention_factor, int max_id) {
    if (contention_factor <= 0.0 || max_id <= 1) { // Uniform distribution if no contention or not enough items
        std::uniform_int_distribution<> dist(1, max_id);
        return EnsureFixedLength<64>(std::to_string(dist(rng)));
    }

    // Simple Zipfian-like selection
    std::vector<int> ids(max_id);
    std::iota(ids.begin(), ids.end(), 1);
    auto sampled_ids = zipf_sample(rng, contention_factor, ids, 1);
    return EnsureFixedLength<64>(std::to_string(sampled_ids[0]));
}

} // namespace movr
} // namespace slog