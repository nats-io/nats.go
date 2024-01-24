package jetstream

// GetObjectShowDeleted makes Get() return object if it was marked as deleted.
func GetObjectShowDeleted() GetObjectOpt {
	return func(opts *getObjectOpts) error {
		opts.showDeleted = true
		return nil
	}
}

// GetObjectInfoShowDeleted makes GetInfo() return object if it was marked as
// deleted.
func GetObjectInfoShowDeleted() GetObjectInfoOpt {
	return func(opts *getObjectInfoOpts) error {
		opts.showDeleted = true
		return nil
	}
}

// ListObjectsShowDeleted makes ListObjects() return deleted objects.
func ListObjectsShowDeleted() ListObjectsOpt {
	return func(opts *listObjectOpts) error {
		opts.showDeleted = true
		return nil
	}
}
