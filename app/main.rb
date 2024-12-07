require 'stringio'
require_relative 'table_page_manager'
require_relative 'index_page_manager'

INTERNAL_TABLE_NAMES = %w[
  sqlite_sequence
  sqlite_schema
].freeze

SCEHMA_TABLE_NAME_COLUMN_IDX = 2
SCEHMA_TABLE_PAGE_NUM_COLUMN_IDX = 3
SCHEMA_TABLE_SQL_COLUMN_INDEX = 4

READ_QUERY_REGEX = /^SELECT (?<column_names>[A-z,\s]+) FROM (?<table_name>[A-z]+)( WHERE (?<where_clause>[A-z\s]+=[A-z\s']+)$|$)/i
COUNT_QUERY_REGEX = /^SELECT COUNT\(\*\) FROM [A-z]+$/i

def main
  @database_file_path = ARGV[0]
  command = ARGV[1]

  @database_file = File.open(@database_file_path, 'rb')
  @table_page_manager = TablePageManager.new(@database_file, db_metadata)
  @index_page_manager = IndexPageManager.new(@database_file, db_metadata)

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
  # TODO: SQL Parser
  return count_query(query_str) if query_str.match?(COUNT_QUERY_REGEX)
  return read_query(query_str) if query_str.match?(READ_QUERY_REGEX)

  raise "Unsupported Query #{query_str}"
end

def count_query(query_str)
  table_name = query_str.split[-1]
  table_data = table_info.find { |ti| ti[:table_name] == table_name }

  raise "Invalid table #{table_name}" unless table_data

  count = 0

  @table_page_manager.paginate(table_data[:page_num]) do |page|
    count += page.record_ids.count
  end

  puts count
end

def read_query(query_str)
  match_data = READ_QUERY_REGEX.match(query_str)

  table_name = match_data['table_name']
  table_data = table_info.find { |ti| ti[:table_name] == table_name }
  raise "Invalid table #{table_name}" unless table_data

  column_names = match_data['column_names'].split(',').map(&:strip)
  where_clause = match_data['where_clause']
  search_col_name, search_col_value = where_clause&.split('=')&.map(&:strip)&.map { |v| v.gsub(/'|"/, '') }
  index = index_info.first { |info| search_col_name && info[:table] == table_name && info[:col] == search_col_name }

  if index
    query_index_scan(table_data, column_names, index, search_col_value)
  else
    query_full_scan(table_data, column_names, search_col_name, search_col_value)
  end
end

def query_index_scan(table_data, column_names, index, search_col_value)
  first_page = index[:page_num]

  indexed_records = []
  @index_page_manager.search_value(first_page, search_col_value, indexed_records)
  indexed_ids = indexed_records.map(&:row_id)
  query_results = []

  @table_page_manager.paginate(table_data[:page_num], indexed_ids) do |page|
    matching_records = page.records.select { |r| indexed_ids.include?(r.first) }
    query_results.concat(select_cols(column_names, table_data, matching_records))
  end

  puts(query_results.map { |v| v.join('|') })
end

def query_full_scan(table_data, column_names, search_col_name, search_col_value)
  query_results = []

  @table_page_manager.paginate(table_data[:page_num]) do |page|
    records = if search_col_name
                filter_records(search_col_name, search_col_value, table_data, page)
              else
                page.records
              end

    query_results.concat(select_cols(column_names, table_data, records))
  end

  puts(query_results.map { |v| v.join('|') })
end

def filter_records(col_name, col_value, table_data, page)
  col_idx = table_data[:col_info].find_index { |ci| ci[:name] == col_name }
  raise "Invalid column name #{name}" unless col_idx

  page.records.select { |r| r[col_idx] == col_value }
end

def select_cols(column_names, table_data, records)
  indexes = column_names.map do |name|
    col_idx = table_data[:col_info].find_index { |ci| ci[:name] == name }
    raise "Invalid column name #{name}" unless col_idx

    col_idx
  end

  records.map { |r| r.values_at(*indexes) }
end

def table_info
  return @table_info unless @table_info.nil?

  page = @table_page_manager.fetch_page(1)
  table_defs = page.records.select { |rv| rv.first == 'table' }

  @table_info = table_defs.map do |td|
    table_create_sql = td[SCHEMA_TABLE_SQL_COLUMN_INDEX]
    col_info = parse_column_info(table_create_sql)

    {
      table_name: td[SCEHMA_TABLE_NAME_COLUMN_IDX],
      page_num: td[SCEHMA_TABLE_PAGE_NUM_COLUMN_IDX],
      col_info: col_info
    }
  end
end

def index_info
  return @index_info unless @index_info.nil?

  page = @table_page_manager.fetch_page(1)
  index_defs = page.records.select { |rv| rv.first == 'index' }

  @index_info = index_defs.map do |id|
    index_create_sql = id[SCHEMA_TABLE_SQL_COLUMN_INDEX]
    parse_index_info(index_create_sql).merge({
                                               page_num: id[SCEHMA_TABLE_PAGE_NUM_COLUMN_IDX]
                                             })
  end
end

def parse_index_info(sql_str)
  regex = /CREATE INDEX (?<idx_name>[^\s]+)\s+on\s*(?<table_name>[^\s]+)\s+\((?<index_col>[^\s]+)\)/
  match_data = regex.match(sql_str)
  raise "Invalid sql #{sql_str}" unless match_data

  {
    name: match_data['idx_name'],
    table: match_data['table_name'],
    col: match_data['index_col']
  }
end

def parse_column_info(sql_str)
  regex = /CREATE TABLE ["A-z]+\s*\(([^)]+)\)/
  match_data = regex.match(sql_str)
  raise "Invalid sql #{sql_str}" unless match_data

  column_lines = match_data[1].split(',')

  column_lines.map do |line|
    name, type, *meta = line.split
    {
      name: name,
      type: type,
      meta: meta
    }
  end
end

main
