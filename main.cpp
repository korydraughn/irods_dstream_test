#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
//#include <irods/transport/udt_transport.hpp>
#include <irods/thread_pool.hpp>
#include <irods/connection_pool.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/plugins/api/get_file_descriptor_info.h>
#include <irods/filesystem.hpp>
#include <irods/query_processor.hpp>

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
#include <iterator>
#include <algorithm>
#include <chrono>

#ifdef USE_FSTREAM
    #define stream_type std::ofstream
    #define the_path "./by_fstream.txt"
#else
    #define stream_type irods::experimental::io::odstream
    #define the_path "/tempZone/home/rods/pt.txt"
#endif

template <typename Transport>
auto put_speed_test(Transport& _tp, const char* _from, const char* _to)
{
    using clock = std::chrono::steady_clock;
    using time_point = std::chrono::time_point<clock>;

    time_point start = clock::now();

    if (std::ifstream in{_from, std::ios_base::binary}; in) {
        if (irods::experimental::io::odstream out{_tp, _to, std::ios_base::binary}; out) {
            char buf[4 * 1024 * 1024]{};

            start = clock::now();

            while (in) {
                in.read(buf, sizeof(buf));
                out.write(buf, in.gcount());
            }

            /*
            std::string line;
            
            start = clock::now();

            while (std::getline(in, line)) {
                out << line;
            }
            */
        }
        else {
            std::cout << "Could not open data object for writing.\n";
        }
    }
    else {
        std::cout << "Could not open file for reading.\n";
    }

    return clock::now() - start;
}

template <typename Transport>
auto get_speed_test(Transport& _tp, const char* _from)
{
    using clock = std::chrono::steady_clock;
    using time_point = std::chrono::time_point<clock>;

    time_point start = clock::now();

    if (irods::experimental::io::idstream in{_tp, _from, std::ios_base::binary}; in) {
        /*
        std::string line;
        std::int64_t count = 0;

        start = clock::now();

        while (std::getline(in, line)) {
            ++count;
        }
        */

        char buf[4 * 1024 * 1024]{};

        start = clock::now();

        while (in) {
            in.read(buf, sizeof(buf));
        }
    }
    else {
        std::cout << "Could not open data object for reading.\n";
    }

    return clock::now() - start;
}

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

#if 0
    try {
        namespace fs = irods::experimental::filesystem;

        irods::connection_pool conn_pool{1, "152.54.6.174", 1247, "rods", "tempZone", 600};
        //irods::connection_pool conn_pool{1, "kdd-ws", 1247, "rods", "tempZone", 600};
        //irods::connection_pool conn_pool{1, "kory-thinkpad", 1247, "rods", "tempZone", 600};

        auto conn = conn_pool.get_connection();

        {
            irods::experimental::io::client::default_transport tp{conn};
            std::cout << "Default Transport - Time Elapsed (Put) = " << std::chrono::duration<double>(put_speed_test(tp, _argv[1], _argv[2])).count() << "s\n";
        }

        {
            irods::experimental::io::client::default_transport tp{conn};
            std::cout << "Default Transport - Time Elapsed (Get) = " << std::chrono::duration<double>(get_speed_test(tp, _argv[2])).count() << "s\n";
        }

        std::cout << '\n';

        {
            irods::experimental::io::client::udt_transport tp{conn};
            fs::path to = _argv[2];
            to += ".udt";
            std::cout << "    UDT Transport - Time Elapsed (Put) = " << std::chrono::duration<double>(put_speed_test(tp, _argv[1], to.c_str())).count() << "s\n";
        }

        {
            irods::experimental::io::client::udt_transport tp{conn};
            std::cout << "    UDT Transport - Time Elapsed (Get) = " << std::chrono::duration<double>(get_speed_test(tp, _argv[2])).count() << "s\n";
        }
    }
    catch (const std::exception& e) {
        std::cout << e.what() << '\n';
    }

    return 0;
#endif

    try {
        {
            using query_proc = irods::query_processor<rcComm_t>;

            const char* query = "select COLL_NAME where COLL_PARENT_NAME = '/tempZone/home'";

            query_proc qp{query, [](const auto& _row) {
                std::cout << "COLL_NAME = " << _row[0] << '\n';
            }};

            irods::thread_pool tp{4};
            irods::connection_pool cp{4, "kdd-ws", 1247, "rods", "tempZone", 600};

            auto errors = qp.execute(tp, cp.get_connection());

            for (auto&& e : errors.get()) {
                std::cout << "ec: " << std::get<0>(e) << ", msg: " << std::get<1>(e) << '\n';
            }

            return 0;
        }

        constexpr int thread_count = 6;
        irods::connection_pool conn_pool{thread_count, host, 1247, "rods", "tempZone", 600};

        // Create the data object if it doesn't exist already.
        // This is required before any parallel transfer occurs.
        // If the data object is not created first, then the transfer hangs.
#ifndef USE_FSTREAM
        {
            auto conn = conn_pool.get_connection();
            irods::experimental::io::client::default_transport dtp{conn};
            //irods::experimental::io::client::udt_transport dtp{conn};
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
                irods::experimental::io::client::default_transport dtp{conn};
                //irods::experimental::io::client::udt_transport dtp{conn};

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
            irods::experimental::io::client::default_transport dtp{conn};
            //irods::experimental::io::client::udt_transport dtp{conn};
            stream_type out{dtp, path, resc, std::ios_base::app};
            char c = '9';
            out.write(&c, 1);

            std::string json = R"_({"fd": )_";
            json += std::to_string(out.file_descriptor());
            json += '}';
            std::cout << "input => " << json << '\n';

            char* json_output{};

            if (const auto ec = rc_get_file_descriptor_info(&static_cast<rcComm_t&>(conn), json.c_str(), &json_output); ec == 0)
                std::cout << "output => " << nlohmann::json::parse(json_output).dump(4) << '\n';
        }

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
