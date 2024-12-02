require_relative 'leaf_index_page'
require_relative 'interior_index_page'

class IndexPageManager
  def initialize(database_file, db_metadata)
    @database_file = database_file
    @db_metadata = db_metadata
  end

  def search_value(page_num, key, results, search_cache = {})
    return if search_cache[page_num]

    page = fetch_page(page_num)

    matched = page.records.select { |r| r.key == key }
    results.concat(matched)

    if page.is_a?(InteriorIndexPage)
      search_pages = find_search_records(page.records, key).map(&:page_num) # found_records.map(&:page_num)
      search_pages.each { |num| search_value(num, key, results, search_cache) }
    end

    search_cache[page_num] = true
  end

  private

  def find_search_records(records, key)
    idx = records.bsearch_index { |r| r.key >= key }
    return [] unless idx

    idx += 1 unless idx == records.length - 1
    records[0..idx]
  end

  def fetch_page(page_num)
    page_size = @db_metadata[:page_size]
    page_offset = (page_num - 1) * page_size
    page_type_offset = page_num == 1 ? 100 : 0

    @database_file.seek(page_offset)
    stream = StringIO.new @database_file.read(page_size)

    stream.seek(page_type_offset)
    page_type = stream.read(1).ord
    raise "Not table tree, page type is #{page_type}" unless [10, 2].include?(page_type)

    stream.seek(0)
    klass = page_type == 10 ? LeafIndexPage : InteriorIndexPage
    klass.new(stream, page_type_offset)
  end
end
