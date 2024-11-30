require_relative 'utils'

InteriorTableRecord = Struct.new(:row_id, :page_num)

class InteriorTablePage
  def initialize(stream, header_offset)
    @stream = stream
    @header_offset = header_offset
  end

  attr_reader :header_offset, :stream

  def child_records
    @child_records ||= cell_infos.map { |ci| InteriorTableRecord.new(ci[:row_id], ci[:page_num]) }
  end

  private

  def cell_infos
    return @cell_infos unless @cell_infos.nil?

    @stream.seek(@header_offset + 3)
    num_cells = @stream.read(2).unpack1('n')

    cell_array_offset = @header_offset + 12

    @cell_infos = (0...num_cells).each_with_object([]) do |i, arr|
      @stream.seek(cell_array_offset + 2 * i)
      cell_offset = @stream.read(2).unpack1('n')
      arr << read_cell_info(cell_offset)
    end
  end

  def read_cell_info(offset)
    @stream.seek(offset)
    page_num = @stream.read(4).unpack1('N')
    row_id = get_var_length(@stream)

    {
      page_num: page_num,
      row_id: row_id
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
end
