#include "execution/movie/load_tables.h"
#include <string>
#include <array>

#include "common/string_utils.h"
#include "execution/movie/table.h"
#include "execution/movie/constants.h"


namespace slog{
namespace movie{

class PartitionedMovieDataLoader {
    public:
        PartitionedMovieDataLoader(const StorageAdapterPtr& storage_adapter)
        : storage_adapter_(storage_adapter) {}
        void Load() {
            LoadMovie();
            LoadUser();
        }
        
    private:
        StorageAdapterPtr storage_adapter_;
        void LoadUser() {
            Table<UserSchema> user(storage_adapter_);
            for(int i = 1; i <= 1000; i++) {
                std::string usernameprefix = std::to_string(i);
                addLeadingZeros(12, usernameprefix);

                std::string postfix = std::to_string(i);
                addLeadingZeros(4, postfix);

                std::string first_name = "first_name_" + postfix;
                std::string last_name = "last_name_" + postfix;
                std::string username = usernameprefix + "_username";
                std::string password = "password_" + postfix;
                int user_id = i;
                user.Insert({
                    MakeFixedTextScalar<21>(username),
                    MakeInt64Scalar(user_id),
                    MakeFixedTextScalar<13>(password),
                    MakeFixedTextScalar<14>(last_name),
                    MakeFixedTextScalar<15>(first_name),
                    MakeInt64Scalar(0L)
                });
            }
        }

        void LoadMovie() {
            Table<MovieSchema> movie(storage_adapter_);
            for(int i = 0; i < movies->length(); i++) {
                std::string titleprefix = std::to_string(i + 1);
                addLeadingZeros(12, titleprefix);

                std::string movie_id = std::to_string(i + 1);
                addLeadingZeros(4, movie_id);
                std::string title = titleprefix + "_" + movies[i];
                addTrailingSpaces(100, title);
                movie.Insert({
                    MakeFixedTextScalar<100>(title),
                    MakeFixedTextScalar<4>(movie_id),
                });
            }
        }
        

        
};

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_regions, int num_partitions, int partition,
                int num_threads) {
                    PartitionedMovieDataLoader loader(storage_adapter);
                    loader.Load();
                }
}
}