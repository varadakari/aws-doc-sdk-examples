#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <fstream>
#include <iostream>

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <bucket-name> <output-file-path>\n";
        return 1;
    }

    const Aws::String bucket_name = argv[1];
    const std::string output_file = argv[2];

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        std::ofstream ofs(output_file);
        if (!ofs.is_open()) {
            std::cerr << "Failed to open output file: " << output_file << "\n";
            return 1;
        }
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.endpointOverride = "https://10.10.10.155:9000";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // Disable only if using self-signed certs
        clientConfig.caFile = "/home/bryck/varada/minio.crt";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";

        
	Aws::S3::S3Client s3_client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()),
			 	    clientConfig,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                    false);

        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(bucket_name);

        Aws::String continuation_token;
        do {
            if (!continuation_token.empty()) {
                request.SetContinuationToken(continuation_token);
            }

            auto outcome = s3_client.ListObjectsV2(request);
            if (!outcome.IsSuccess()) {
                std::cerr << "ListObjects failed: " << outcome.GetError().GetMessage() << "\n";
                break;
            }

            const auto& result = outcome.GetResult();
            for (const auto& object : result.GetContents()) {
                ofs << object.GetKey() << "\n";
            }

            continuation_token = result.GetNextContinuationToken();
        } while (!continuation_token.empty());

        ofs.close();
        std::cout << "Object list written to: " << output_file << "\n";
    }
    Aws::ShutdownAPI(options);
    return 0;
}

