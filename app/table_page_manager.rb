require_relative 'leaf_table_page'
require_relative 'interior_table_page'

class TablePageManager
  def initialize(database_file, db_metadata)
    @database_file = database_file
    @db_metadata = db_metadata
  end

  def fetch_page(page_num)
    load_page(page_num) do |data|
      page_type, stream, header_offset = data.values_at(:page_type, :stream, :header_offset)
      klass = page_type == :leaf ? LeafTablePage : InteriorTablePage
      return klass.new(stream, header_offset)
    end
  end

  def paginate(start_page_num, &block)
    page = fetch_page(start_page_num)

    if page.is_a?(LeafTablePage)
      yield page
    else
      page.child_records.each do |record|
        paginate(record.page_num, &block)
      end
    end
  end

  private

  def load_page(page_num)
    page_size = @db_metadata[:page_size]
    @database_file.seek((page_num - 1) * page_size)
    stream = StringIO.new @database_file.read(page_size)

    page_type_offset = page_num == 1 ? 100 : 0
    stream.seek(page_type_offset)
    page_type = stream.read(1).ord
    raise "Not table tree, page type is #{page_type}" unless [13, 5].include?(page_type)

    stream.seek(0)

    data = {
      page_type: page_type == 5 ? :interior : :leaf,
      header_offset: page_type_offset,
      stream: stream
    }

    yield data
    stream.close
  end
end
