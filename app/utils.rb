class Utils
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

  def self.read_col_value(stream, type_code)
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

    bytes = stream.read(byte_length)

    return bytes.to_s if klass == String
    return bytes.unpack1('g') if klass == Float
    return unless klass == Integer

    convert_to_signed_int(bytes, byte_length)
  end

  def self.convert_to_signed_int(bytes, length)
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
end
