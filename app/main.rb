def main
  database_file_path = ARGV[0]
  command = ARGV[1]

  case command
  when '.dbinfo'
    dbinfo(database_file_path)
  else
    raise 'Invalid command'
  end
end

def dbinfo(database_file_path)
  File.open(database_file_path, 'rb') do |database_file|
    database_file.seek(16) # Skip the first 16 bytes of the header
    page_size = database_file.read(2).unpack1('n')

    # Skip rest of dabase file header
    database_file.pos += 82

    # Assumptions:
    # * no free pages
    # * first b tree page is sqlite_schema
    database_file.pos += 3
    num_tables = database_file.read(2).unpack1('n')

    puts "database page size: #{page_size}"
    puts "number of tables: #{num_tables}"
  end
end

main
