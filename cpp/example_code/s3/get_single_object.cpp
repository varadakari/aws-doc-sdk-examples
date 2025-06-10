#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>

#include <iostream>
#include <fcntl.h>    // open flags
#include <unistd.h>   // write, close
#include <cstdlib>    // posix_memalign
#include <cstring>    // memset
#include <cerrno>

static const size_t ALIGNMENT = 4096;       // common alignment for Direct I/O
static const size_t BUFFER_SIZE = 2 * 1024 * 1024; // 2MB buffer size, multiple of ALIGNMENT

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <bucket-name> <object-key> <output-file-path>\n";
        return 1;
    }

    const Aws::String bucket_name = argv[1];
    const Aws::String object_key = argv[2];
    const char* output_file = argv[3];

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.endpointOverride = "https://10.10.10.155:9000";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // Disable only if using self-signed certs
        clientConfig.caFile = "/home/ubuntu/varada/minio.crt";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";

        
	Aws::S3::S3Client s3_client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()),
			 	    clientConfig,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                    false);

        Aws::S3::Model::GetObjectRequest get_req;
        get_req.SetBucket(bucket_name);
        get_req.SetKey(object_key);

        auto get_outcome = s3_client.GetObject(get_req);
        if (!get_outcome.IsSuccess()) {
            std::cerr << "GetObject failed: " << get_outcome.GetError().GetMessage() << "\n";
            Aws::ShutdownAPI(options);
            return 1;
        }

        int fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        if (fd < 0) {
            std::cerr << "Failed to open output file with O_DIRECT: " << strerror(errno) << "\n";
            Aws::ShutdownAPI(options);
            return 1;
        }

        void* buffer = nullptr;
        int rc = posix_memalign(&buffer, ALIGNMENT, BUFFER_SIZE);
        if (rc != 0 || buffer == nullptr) {
            std::cerr << "posix_memalign failed\n";
            close(fd);
            Aws::ShutdownAPI(options);
            return 1;
        }

        Aws::IOStream& stream = get_outcome.GetResultWithOwnership().GetBody();

        size_t total_bytes_written = 0;
        while (!stream.eof()) {
            memset(buffer, 0, BUFFER_SIZE);

            stream.read(static_cast<char*>(buffer), BUFFER_SIZE);
            std::streamsize bytes_read = stream.gcount();

            if (bytes_read <= 0) {
                break;
            }

            size_t to_write = bytes_read;
            size_t write_size = ((to_write + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;

            ssize_t written = 0;
            size_t offset = 0;
            while (offset < write_size) {
                ssize_t ret = write(fd, (char*)buffer + offset, write_size - offset);
                if (ret < 0) {
                    std::cerr << "Write error: " << strerror(errno) << "\n";
                    free(buffer);
                    close(fd);
                    Aws::ShutdownAPI(options);
                    return 1;
                }
                offset += ret;
                written += ret;
            }
            total_bytes_written += to_write;
        }

        free(buffer);
        close(fd);

        std::cout << "Downloaded object '" << object_key << "' from bucket '" << bucket_name 
                  << "' with Direct I/O to file '" << output_file 
                  << "', total bytes written: " << total_bytes_written << "\n";
    }
    Aws::ShutdownAPI(options);
    return 0;
}

