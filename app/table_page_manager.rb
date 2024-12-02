require_relative 'leaf_table_page'
require_relative 'interior_table_page'

class TablePageManager
  def initialize(database_file, db_metadata)
    @database_file = database_file
    @db_metadata = db_metadata
  end

  def fetch_page(page_num)
    create_page(page_num)
  end

  def paginate(start_page_num, filter_ids = nil, &block)
    page = fetch_page(start_page_num)

    if page.is_a?(LeafTablePage)
      yield page
    else
      records = if filter_ids.nil?
                  page.child_records
                else
                  filtered_records_by_ids(filter_ids, page.child_records)
                end

      records.each { |record| paginate(record.page_num, filter_ids, &block) }

      paginate(page.right_page, filter_ids, &block)
    end
  end

  private

  def filtered_records_by_ids(filter_ids, records)
    idx = filter_ids.map do |row_id|
      records.bsearch_index { |r| r.row_id >= row_id }
    end.compact.max

    records.select.with_index { |_, i| !idx.nil? && i <= idx }
  end

  def create_page(page_num)
    page_size = @db_metadata[:page_size]
    page_offset = (page_num - 1) * page_size
    page_type_offset = page_num == 1 ? 100 : 0

    @database_file.seek(page_offset)
    stream = StringIO.new @database_file.read(page_size)

    stream.seek(page_type_offset)
    page_type = stream.read(1).ord
    raise "Not table tree, page type is #{page_type}" unless [13, 5].include?(page_type)

    stream.seek(0)
    klass = page_type == 13 ? LeafTablePage : InteriorTablePage
    klass.new(stream, page_type_offset)
  end
end
