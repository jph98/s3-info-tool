#!/usr/bin/env ruby

require "rubygems"

require "aws-sdk"
require "yaml"

# See: https://github.com/aws/aws-sdk-core-ruby
class S3Info

        def initialize(config_file_location, bucket_name)

                config = YAML::load(File.open(config_file_location))

                AWS.config(:region => config["region"],
                                   :s3_endpoint => config["endpoint"],
                                   :access_key_id => config["access_key_id"],
                                   :secret_access_key => config["secret_access_key"])

                @client = AWS::S3.new()

                @bucket_name = bucket_name
        end

        def summarise_servers()

                puts "\nTop level bucket: #{@bucket_name}\n========================================\n"

                tree = @client.buckets[@bucket_name].as_tree

                directories = tree.children.select(&:branch?).collect(&:prefix)
                directories.each do |d|
                        count = count_objects(d)
                        puts "------------------------------------------------------------"
                end
        end

        # List all objects for a key in the given bucket_name
        def list_objects(key)

                puts "Objects in bucket #{@bucket_name}/#{key}:"
                files = @client.buckets[@bucket_name].objects.with_prefix(key)

                resp = files.each do |o|
                  puts "\t#{o.key} => #{o.etag}"
                end
        end

        # Count all objects for a key in the given bucket name
        def count_objects(key)

                puts "Bucket: #{@bucket_name} - key: #{key}\n"

                files = @client.buckets[@bucket_name].
                                                objects.with_prefix(key)

                count = 0
                files.each do |o|
                        count += 1
                end
                puts "Count of files in #{@bucket_name}/#{key}: #{count}"
                return count
        end

        def check_object(key)

                count = count_objects(key)
                if count < 1
                        puts "\nCould not find object\n\n"
                else
                        puts "\nFound Object\n\n"
                end
        end

end

usage = "Usage: $0 <config_file_location> <bucket_name> <key> <count|list|summarise|checkobject>"
raise("Need config file location: " + usage) if !ARGV[0]
raise("Need directory: " + usage) if !ARGV[1]
raise("Need action: " + usage) if !ARGV[2]

config_file_location = ARGV[0]
bucket_name = ARGV[1]
action = ARGV[2]

if action != "summarise"
        raise("Need key: " + usage) if !ARGV[3]
        key = ARGV[3]
end

info = S3Info.new(config_file_location, bucket_name)
if action == "count"
        info.count_objects(key)
elsif action == "list"
        info.list_objects(key)
elsif action == "summarise"
        info.summarise_servers()
elsif action == "checkobject"
        raise("Need objectname (e.g. ui-2/53e46451467.gz " + usage) if !ARGV[3]
        objectname = ARGV[3]
        info.check_object(objectname)
else
        puts "No action specified: #{usage}"
end
