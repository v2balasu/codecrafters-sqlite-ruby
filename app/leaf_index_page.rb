require_relative 'utils'

LeafIndexRecord = Struct.new(:key, :row_id, :overflow_page_num)

class LeafIndexPage
  def initialize(stream, header_offset)
    @stream = stream
    @header_offset = header_offset
  end

  attr_reader :header_offset, :stream

  def record_values
    return @record_values unless @record_values.nil?

    @record_values = cell_infos.map do |ci|
      content = read_cell_content(ci[:record_body_offset])
      *keys, row_id = content[:values]

      LeafIndexRecord.new(keys.join(''), row_id, content[:overflow_page_num])
    end
  end

  # private
  def cell_infos
    return @cell_infos unless @cell_infos.nil?

    @stream.seek(@header_offset + 3)
    num_cells = @stream.read(2).unpack1('n')

    cell_array_offset = @header_offset + 8

    @cell_infos = (0...num_cells).each_with_object([]) do |i, arr|
      @stream.seek(cell_array_offset + 2 * i)
      cell_offset = @stream.read(2).unpack1('n')
      arr << read_cell_info(cell_offset)
    end
  end

  def read_cell_info(offset)
    @stream.seek(offset)
    record_size = get_var_length(@stream)

    {
      record_body_offset: @stream.pos,
      record_size: record_size
    }
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

  def read_cell_content(offset)
    @stream.seek(offset)
    header_length = get_var_length(@stream)
    bytes_to_read = header_length - (header_length.bit_length / 8.00).ceil

    start_pos = @stream.pos
    bytes_processed = 0
    column_type_codes = []

    while bytes_processed < bytes_to_read
      column_type_codes << get_var_length(@stream)
      bytes_processed += @stream.pos - start_pos
      start_pos = @stream.pos
    end

    values = column_type_codes.map { |code| Utils.read_col_value(@stream, code) }
    overflow_page = @stream.read(4)&.unpack1('N')
    {
      overflow_page: overflow_page,
      values: values
    }
  end
end
