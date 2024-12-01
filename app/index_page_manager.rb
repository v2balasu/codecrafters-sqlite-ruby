require_relative 'leaf_index_page'
require_relative 'interior_index_page'

class IndexPageManager
  def initialize(database_file, db_metadata)
    @database_file = database_file
    @db_metadata = db_metadata
  end

  def fetch_page(page_num)
    load_page(page_num) do |data|
      page_type, stream, header_offset = data.values_at(:page_type, :stream, :header_offset)
      klass = page_type == :leaf ? LeafIndexPage : InteriorIndexPage
      return klass.new(stream, header_offset)
    end
  end

  def paginate_search_records(start_page_num, search_value, &block)
    page = fetch_page(start_page_num)
    results = find_in_page(page, search_value)
    yield results if results

    return unless page.is_a?(InteriorIndexPage)

    puts 'PROCESS INTERIOR PAGE'
    page.record_values.each { |r| paginate_search_records(r.page_num, search_value, &block) }
  end

  def find_in_page(page, search_value)
    search_idx = page.record_values.bsearch_index { |r| r.key >= search_value }
    return nil unless search_idx && page.record_values[search_idx].key == search_value

    # puts "SEARCH IDX #{search_idx} #{page.record_values.length}"
    index_records = [page.record_values[search_idx]]

    idx = search_idx + 1
    while idx < page.record_values.length && page.record_values[idx].key == search_value
      index_records << page.record_values[idx]
      idx += 1
    end

    idx = search_idx - 1
    while idx >= 0 && page.record_values[idx].key == search_value
      index_records << page.record_values[idx]
      idx -= 1
    end

    index_records
  end

  def paginate(start_page_num, &block)
    page = fetch_page(start_page_num)

    if page.is_a?(LeafIndexPage)
      yield page
      page.record_values.map(&:overflow_page_num).compact.each do |op|
        paginate(op, &block)
      end
    else
      puts 'PROCESS INTERIOR PAGE'
      page.record_values.each do |record|
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
    raise "Not table tree, page type is #{page_type}" unless [10, 2].include?(page_type)

    stream.seek(0)

    data = {
      page_type: page_type == 2 ? :interior : :leaf,
      header_offset: page_type_offset,
      stream: stream
    }

    yield data
    stream.close
  end
end
