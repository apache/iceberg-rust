#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Accessor {
    position: usize,
    inner: Option<Box<Accessor>>,
}

impl Accessor {
    pub(crate) fn new(position: usize, inner: Option<Accessor>) -> Self {
        Accessor {
            position,
            inner: inner.map(Box::new),
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    // fn get(&self, container: T) -> R {
    //     let mut val = container[self.position];
    //     let mut inner = &self.inner;
    //
    //     while let Some(inner_inner) = inner {
    //         val = val[inner_inner.position];
    //         inner = &inner_inner.inner;
    //     }
    //
    //     val
    // }
}
