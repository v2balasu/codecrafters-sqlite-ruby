require_relative 'utils'

class LeafTablePage
  def initialize(stream, header_offset)
    @stream = stream
    @header_offset = header_offset
  end

  attr_reader :header_offset, :stream

  def record_ids
    @record_ids ||= cell_infos.map { |ci| ci[:row_id] }
  end

  def records
    return @records unless @records.nil?

    @records = cell_infos
               .map { |ci| read_cell_content(ci[:record_body_offset], ci[:row_id]) }
  end

  private

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
    row_id = get_var_length(@stream)

    {
      row_id: row_id,
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

  def read_cell_content(offset, row_id)
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

    column_values = column_type_codes.map { |code| Utils.read_col_value(@stream, code) }

    # hack
    column_values[0] = row_id if column_values.first.nil?

    column_values
  end
end
