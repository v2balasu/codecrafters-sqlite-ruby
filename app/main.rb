require 'stringio'

COLUMN_TYPE_CODE_MAPPING = {
  0 => {
    klass: NilClass,
    byte_length: 0
  },
  1 => {
    klass: Integer,
    byte_length: 1
  },
  2 => {
    klass: Integer,
    byte_length: 2
  },
  3 => {
    klass: Integer,
    byte_length: 3
  },
  4 => {
    klass: Integer,
    byte_length: 4
  },
  5 => {
    klass: Integer,
    byte_length: 6
  },
  6 => {
    klass: Integer,
    byte_length: 8
  },
  7 => {
    klass: Float,
    byte_length: 8
  }
}.freeze

INTERNAL_TABLE_NAMES = %w[
  sqlite_sequence
  sqlite_schema
].freeze

SCEHMA_TABLE_NAME_COLUMN_IDX = 2
SCEHMA_TABLE_PAGE_NUM_COLUMN_IDX = 3

def main
  @database_file_path = ARGV[0]
  command = ARGV[1]

  case command
  when '.dbinfo'
    dbinfo
  when '.tables'
    tables
  else
    query(command)
  end
end

def db_metadata
  @db_metadata ||= File.open(@database_file_path, 'rb') do |database_file|
    database_file.seek(16) # Skip the first 16 bytes of the header
    page_size = database_file.read(2).unpack1('n')

    # Skip rest of dabase file header
    database_file.pos += 82

    # Assumptions:
    # * no free pages
    # * first b tree page is sqlite_schema
    database_file.pos += 3
    num_tables = database_file.read(2).unpack1('n')

    {
      page_size: page_size,
      num_tables: num_tables
    }
  end
end

def dbinfo
  puts "database page size: #{db_metadata[:page_size]}"
  puts "number of tables: #{db_metadata[:num_tables]}"
end

def tables
  table_names = table_info.map { |ti| ti[:table_name] }
  table_names.reject! { |name| INTERNAL_TABLE_NAMES.include?(name) }
  puts table_names.join("\s")
end

def query(query_str)
  raise "Unsupported Query #{query_str}" unless query_str.match?(/^SELECT COUNT\(\*\) FROM [a-z]+$/i)

  table_name = query_str.split[-1]
  table_data = table_info.find { |ti| ti[:table_name] == table_name }

  raise "Invalid table #{table_name}" unless table_data

  puts get_page_cell_data(table_data[:page_num]).count
end

def table_info
  return @table_info unless @table_info.nil?

  @table_info = get_page_cell_data(1).map do |cd|
    {
      table_name: cd[:column_values][SCEHMA_TABLE_NAME_COLUMN_IDX],
      page_num: cd[:column_values][SCEHMA_TABLE_PAGE_NUM_COLUMN_IDX]
    }
  end
end

def get_page_cell_data(page_num)
  page_stream = get_page(page_num - 1)
  page_header_offset = page_num == 1 ? 100 : 0
  cell_count_offset = page_header_offset + 3
  cell_array_offset = page_header_offset + 8

  page_stream.seek(page_header_offset)
  page_type = page_stream.read(1).ord
  raise 'Page is not a leaf table b-tree page' unless page_type == 13

  page_stream.seek(cell_count_offset)
  num_cells = page_stream.read(2).unpack1('n')

  (0...num_cells).each_with_object([]) do |i, arr|
    page_stream.seek(cell_array_offset + 2 * i)
    cell_offset = page_stream.read(2).unpack1('n')
    arr << read_cell(page_stream, cell_offset)
  end
end

def read_cell(page_stream, offset)
  page_stream.seek(offset)
  _record_size = get_var_length(page_stream)
  row_id = get_var_length(page_stream)
  record_data = read_record(page_stream)

  {
    row_id: row_id,
    column_values: record_data[:column_values]
  }
end

def read_record(page_stream)
  header_length = get_var_length(page_stream)
  bytes_to_read = header_length - (header_length.bit_length / 8.00).ceil

  start_pos = page_stream.pos
  bytes_processed = 0
  column_type_codes = []

  while bytes_processed < bytes_to_read
    column_type_codes << get_var_length(page_stream)
    bytes_processed += page_stream.pos - start_pos
    start_pos = page_stream.pos
  end

  column_values = column_type_codes.map { |code| read_col_value(page_stream, code) }

  {
    column_type_codes: column_type_codes,
    column_values: column_values
  }
end

def read_col_value(page_stream, type_code)
  raise 'Unsupported type' if [10, 11].include?(type_code)

  return 0 if type_code == 8
  return 1 if type_code == 9
  return nil if type_code.zero?

  klass, byte_length = if type_code <= 7
                         COLUMN_TYPE_CODE_MAPPING[type_code].values_at(:klass, :byte_length)
                       elsif type_code.even?
                         [String, (type_code - 12) / 2]
                       else
                         [String, (type_code - 13) / 2]
                       end

  bytes = page_stream.read(byte_length)

  return bytes.to_s if klass == String
  return bytes.unpack1('g') if klass == Float
  return unless klass == Integer

  convert_to_signed_int(bytes, byte_length)
end

def convert_to_signed_int(bytes, length)
  bits = bytes.unpack("C#{length}").reverse.reduce(0) do |curr, byte|
    (curr << 8) | byte
  end

  is_signed = (bits >> (length * 8 - 1)) == 1

  if is_signed
    -1 * (bits - (1 << length * 8 - 1))
  else
    bits
  end
end

def get_page(page_num)
  page_size = db_metadata[:page_size]

  File.open(@database_file_path, 'rb') do |database_file|
    database_file.seek(page_num * page_size)
    StringIO.new database_file.read(page_size)
  end
end

def get_var_length(buffer)
  length = 0

  byte = 0

  8.times do
    byte = buffer.read(1).ord
    length = (length << 7) | (byte & 0b01111111)
    break if (byte & 0b10000000).zero?
  end

  length = (length << 8) | byte unless (byte & 0b10000000).zero?

  length
end

main
