package record

func NewUntyped(key, value any, meta Metadata) *UntypedRecord {
	return &UntypedRecord{
		Key:      key,
		Value:    value,
		Metadata: meta,
	}
}

func (r *Record[K, V]) ToUntyped() *UntypedRecord {
	return &UntypedRecord{
		Key:      r.Key,
		Value:    r.Value,
		Metadata: r.Metadata,
	}
}
