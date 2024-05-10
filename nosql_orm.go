package nosqlorm

type NoSqlOrm[T any] interface {
	Insert(T) error
	Select(T) ([]T, error)
	Update(T) error
	Delete(T) error
}
