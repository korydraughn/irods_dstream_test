#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/connection_pool.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/plugins/api/get_file_descriptor_info.h>

#include "json.hpp"

#include <iostream>
#include <functional>
#include <utility>
#include <thread>
#include <vector>
#include <memory>
#include <mutex>
#include <fstream>
#include <string>

#ifdef USE_FSTREAM
    #define stream_type std::ofstream
    #define the_path "./by_fstream.txt"
#else
    #define stream_type irods::experimental::odstream
    #define the_path "/tempZone/home/rods/pt.txt"
#endif

int main(int _argc, char* _argv[])
{
    if (_argc < 3)
    {
        std::cerr << "Usage: ./" + std::string{_argv[0]} + " <host> <resc_name>\n";
        return 1;
    }

    const char* host = _argv[1];
    const char* resc = _argv[2];
    const char* path = the_path;

    auto& api_table = irods::get_client_api_table();
    auto& pck_table = irods::get_pack_table();
    init_api_table(api_table, pck_table);

    try {
        constexpr int thread_count = 6;
        irods::connection_pool conn_pool{thread_count, host, 1247, "rods", "tempZone", 600};

        // Create the data object if it doesn't exist already.
        // This is required before any parallel transfer occurs.
        // If the data object is not created first, then the transfer hangs.
#ifndef USE_FSTREAM
        {
            auto conn = conn_pool.get_connection();
            irods::experimental::default_transport dtp{conn};
            stream_type{dtp, path, resc};
        }
#else
        { stream_type{path}; }
#endif

        std::vector<std::thread> thread_pool;
        std::mutex m;

        constexpr char chars[] = {'A', 'B', 'C', 'X', 'Y', 'Z'};
        constexpr int thread_count_minus_one = thread_count - 1;

        for (int i = 0; i < thread_count; ++i)
        {
            thread_pool.emplace_back([&conn_pool, &m, &chars, i, resc, path] {
                int bytes_to_write = 4096;

                std::vector<char> buf(bytes_to_write - 1, chars[i]);
                buf.push_back('\n');

                auto conn = conn_pool.get_connection();
                irods::experimental::default_transport dtp{conn};

#ifndef USE_FSTREAM
                stream_type out{dtp, path, resc, std::ios_base::in | std::ios_base::out};
#else
                stream_type out{path, std::ios_base::in | std::ios_base::out};
#endif

                if (!out)
                {
                    std::lock_guard l{m};
                    std::cerr << "open error on " << chars[i] << '\n';
                }

                if (!out.seekp(i * bytes_to_write))
                {
                    std::lock_guard l{m};
                    std::cerr << "seek error on " << chars[i] << '\n';
                }

                // Write one less byte if the you're the last thread.
                if (i == thread_count_minus_one)
                    --bytes_to_write;

                if (!out.write(buf.data(), bytes_to_write))
                {
                    std::lock_guard l{m};
                    std::cerr << "write error on " << chars[i] << '\n';
                }

                std::lock_guard l{m};
                std::cerr << "thread '" << chars[i] << "' finished!\n";
            });
        }

        for (auto&& t : thread_pool)
            t.join();

        {
            // Write the last byte to the data object to guarantee that
            // the correct size is recorded in the catalog.
            auto conn = conn_pool.get_connection();
            irods::experimental::default_transport dtp{conn};
            stream_type out{dtp, path, resc, std::ios_base::app};
            char c = '9';
            out.write(&c, 1);
        }

        std::string json = R"_({"fd": )_";
        json += std::to_string(out.file_descriptor());
        json += '}';
        std::cout << "input => " << json << '\n';

        char* json_output{};

        if (const auto ec = rc_get_file_descriptor_info(&static_cast<rcComm_t&>(conn), json.c_str(), &json_output); ec == 0)
            std::cout << "output => " << nlohmann::json::parse(json_output).dump(4) << '\n';

        /*
         What might a parallel dstream interface look like?

         auto parallel_dstream(const fs::path& _from,
                               const fs::path& _to,
                               int _streams = <some_default>,
                               int _stream_buffer_size = <some_default>) -> parallel_result_type
         */
    }
    catch (const std::exception& e) {
        std::cout << e.what() << '\n';
    }

    return 0;
}
