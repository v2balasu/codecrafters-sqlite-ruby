database_file_path = ARGV[0]
command = ARGV[1]

if command == ".dbinfo"
  File.open(database_file_path, "rb") do |database_file|
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    $stderr.puts "Logs from your program will appear here"

    database_file.seek(16)  # Skip the first 16 bytes of the header
    page_size = database_file.read(2).unpack("n")[0]

    # Skip rest of dabase file header
    database_file.pos += 82

    # Assumptions: 
    # * no free pages
    # * first b tree page is sqlite_schema
    database_file.pos += 3
    num_tables = database_file.read(2).unpack("n")[0]

    puts "database page size: #{page_size}"
    puts "number of tables: #{num_tables}"
  end
end
