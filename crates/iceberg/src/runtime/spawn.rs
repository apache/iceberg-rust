struct TaskHandle {}

impl TaskHandle {
    pub fn new() -> Self {
        Self {}
    }
}

pub fn spawn() -> TaskHandle {
    TaskHandle::new()
}