package kafka

import "encoding/binary"

type AvroSerde struct {
	Serdes
}

// Serialize serializes a JSON object into Avro binary.
func (*AvroSerde) Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError) {
	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, err
	}

	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schema.ID))

	encodedData, _, originalErr := schema.Codec().NativeFromTextual(jsonBytes)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to encode data", originalErr)
	}

	bytesData, originalErr := schema.Codec().BinaryFromNative(nil, encodedData)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncodeToBinary,
			"Failed to encode data into binary",
			originalErr)
	}

	var binaryMsg []byte
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	//avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, bytesData...)

	return binaryMsg, nil
}

// Deserialize deserializes a Avro binary into a JSON object.
func (*AvroSerde) Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError) {
	decodedData, _, err := schema.Codec().NativeFromBinary(data)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedToDecodeFromBinary, "Failed to decode data", err)
	}

	if data, ok := decodedData.(map[string]interface{}); ok {
		return data, nil
	} else {
		return nil, ErrInvalidDataType
	}
}
