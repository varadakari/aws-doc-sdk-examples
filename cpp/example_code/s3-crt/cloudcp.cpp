#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/FileSystemUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <memory>
#include <regex>
#include <dirent.h>

bool parseS3Uri(const std::string& uri, std::string& bucket, std::string& prefix, std::string& objectKey) {
    const std::string prefix_str = "s3://";
    if (uri.substr(0, prefix_str.length()) != prefix_str) return false;

    std::string rest = uri.substr(prefix_str.length());
    size_t firstSlash = rest.find('/');

    if (firstSlash == std::string::npos) {
        // No object path given
        bucket = rest;
        prefix.clear();
        objectKey.clear();
        return true;
    }

    bucket = rest.substr(0, firstSlash);
    std::string keyPath = rest.substr(firstSlash + 1);  // everything after bucket/

    size_t lastSlash = keyPath.find_last_of('/');
    if (lastSlash == std::string::npos) {
        // No prefix, only object
        prefix.clear();
        objectKey = keyPath;
    } else {
        prefix = keyPath.substr(0, lastSlash);
        objectKey = keyPath.substr(lastSlash + 1);
    }

    return true;
}

Aws::String HtmlUnescape(const Aws::String& input) {
    Aws::String output = input;
    size_t pos;
    while ((pos = output.find("&#34;")) != Aws::String::npos) {
        output.replace(pos, 5, "\"");
    }
    while ((pos = output.find("&quot;")) != Aws::String::npos) {
        output.replace(pos, 6, "\"");
    }
    // Add more replacements as needed
    return output;
}

// Upload file to S3
int UploadFile(const std::string& bucket, const std::string& object_key, const std::string& file_path, const Aws::S3Crt::S3CrtClient& client) {
    struct stat st;
    if (stat(file_path.c_str(), &st) != 0) {
        std::cerr << "File stat failed\n";
        return 1;
    }

    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object_key);
    auto input_data = Aws::MakeShared<Aws::FStream>("UploadStream", file_path.c_str(), std::ios::binary | std::ios::in);
    if (!input_data->good()) {
        std::cerr << "Failed to open file: " << file_path << std::endl;
        return 1;
    }
    request.SetBody(input_data);

    auto outcome = client.PutObject(request);
    if (outcome.IsSuccess()) {
        //Aws::String etagRaw = HtmlUnescape(outcome.GetResult().GetETag());
        //std::cout << etagRaw << std::endl;
    } else {
        std::cerr << "PutObject failed: " << outcome.GetError().GetMessage() << std::endl;
        return 1;
    }

    return 0;
}

bool make_parent_dirs(const std::string& path) {
    size_t pos = 0;
    while ((pos = path.find('/', pos + 1)) != std::string::npos) {
        std::string dir = path.substr(0, pos);
        if (!dir.empty()) {
            mkdir(dir.c_str(), 0755);
        }
    }
    return true;
}

int DownloadFile(const Aws::S3Crt::S3CrtClient& client, const std::string& bucket, const Aws::String& key, const std::string& target_base) {
    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);
    std::string full_path = target_base;
    make_parent_dirs(full_path);

    request.SetResponseStreamFactory([=]() {
            return Aws::New<Aws::FStream>("GETOBJ", full_path.c_str(), std::ios::out | std::ios::binary);
    });

    auto getObjectOutcome = client.GetObject(request);

    if(getObjectOutcome.IsSuccess()) {
        //std::cout << getObjectOutcome.GetResult().GetETag() << std::endl;
    } else {
        const auto& err = getObjectOutcome.GetError();
        std::cerr << "Failed to download: " << err.GetExceptionName()
                  << " - " << err.GetMessage() << std::endl;
        return 1;
    }

    return 0;
}

bool file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

int main(int argc, char** argv) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        std::string endpointUrl;
        std::vector<std::string> positionalArgs;

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];

            if (arg == "--endpoint-url") {
                if (i + 1 < argc) {
                    endpointUrl = argv[++i];
                } else {
                    std::cerr << "--endpoint-url requires a value\n";
                    Aws::ShutdownAPI(options);
                    return 1;
                }
            } else if (arg.rfind("-", 0) == 0) {
                // Unknown option, skip silently
                continue;
            } else {
                // Positional argument
                if (positionalArgs.size() < 2) {
                    positionalArgs.push_back(arg);
                } else {
                    // More than two positional args, ignore or handle error if needed
                    // For now, just ignore extra positional args
                }
            }
        }

        if (positionalArgs.size() != 2) {
            std::cerr << "Usage:\n"
                      << "  cloudcp [--endpoint-url <url>] /path/to/file s3://bucket[/prefix/]\n"
                      << "  cloudcp [--endpoint-url <url>] s3://bucket[/prefix/] /path/to/file\n";
            Aws::ShutdownAPI(options);
            return 1;
        }

        std::string src = positionalArgs[0];
        std::string dst = positionalArgs[1];


        Aws::S3Crt::S3CrtClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true; // use with caution
        config.useVirtualAddressing = false; // for non-AWS S3
        config.enableTcpKeepAlive = true;
        config.tcpKeepAliveIntervalMs = 30000;
        if (!endpointUrl.empty()) {
            config.endpointOverride = endpointUrl.c_str();
        }
        const std::string ca_cert_path = "/usr/local/share/ca-certificates/minio.crt";
        if (file_exists(ca_cert_path)) {
            config.caFile = ca_cert_path;
        }

        //std::shared_ptr<S3CrtClient> client = Aws::MakeShared<S3CrtClient>("S3CRTClient", creds, config);
        Aws::S3Crt::S3CrtClient s3(config);

        std::string bucket, prefix, objectKey;

        if (parseS3Uri(src, bucket, prefix, objectKey)) {
            // Download
            std::string localPath = dst;
            std::string key = prefix.empty() ? objectKey : (prefix + "/" + objectKey);
            if (DownloadFile(s3, bucket, key, localPath)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
        } else if (parseS3Uri(dst, bucket, prefix, objectKey)) {
            // Upload
            std::string localPath = src;
            std::string filename = localPath.substr(localPath.find_last_of("/\\") + 1);
            std::string key = prefix.empty() ? filename : (prefix + "/" + filename);
            if (UploadFile(bucket, key, localPath, s3)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
        } else {
            std::cerr << "Invalid source or destination. One must be s3://bucket/key\n";
            Aws::ShutdownAPI(options);
            return 1;
        }
    }
    Aws::ShutdownAPI(options);
    return 0;
}
