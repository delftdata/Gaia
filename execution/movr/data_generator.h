#include <random>
#include <string>
#include <utility>
#include <vector>

namespace slog {
namespace movr {

class DataGenerator {
public:
    template <typename T>
    static T WeightedChoice(std::mt19937& rng, const std::vector<std::pair<T, double>>& items) {
      double total_weight = 0.0;
    for (const auto& item : items) total_weight += item.second;

    std::uniform_real_distribution<double> dist(0.0, total_weight);
    double n = dist(rng);

    for (const auto& [value, weight] : items) {
        if (n < weight) return value;
        n -= weight;
    }

    return items.back().first; // fallback in case of rounding errors
    }

    template<size_t N>
    static std::string EnsureFixedLength(const std::string& input) {
        if (input.size() == N) {
            return input;
        }
        
        if (input.size() > N) {
            // Truncate the string
            return input.substr(0, N);
        }
        
        // Pad the string with spaces
        std::string padded = input;
        padded.resize(N, ' ');
        return padded;
    }

    static std::string GenerateRevenue(std::mt19937& rng);
    static std::string GenerateRandomVehicleType(std::mt19937& rng);
    static std::string GetVehicleAvailability(std::mt19937& rng);
    static std::string GenerateRandomColor(std::mt19937& rng);
    static std::pair<std::string, std::string> GenerateRandomLatLong(std::mt19937& rng);
    static std::string GenerateBikeBrand(std::mt19937& rng);
    static std::string GenerateVehicleMetadata(std::mt19937& rng, const std::string& type);
    static std::string GenerateName(std::mt19937& rng);
    static std::string GenerateAddress(std::mt19937& rng);
    static std::string GenerateCreditCard(std::mt19937& rng);
    static std::string GeneratePromoCode(std::mt19937& rng);
    static std::string GenerateDescription(std::mt19937& rng);
    static std::string GenerateRules(std::mt19937& rng);
    static std::string GenerateContendedID(std::mt19937& rng, double contention_factor, int max_id);
};

} // namespace movr
} // namespace slog
