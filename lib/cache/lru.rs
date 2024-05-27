use std::{collections::HashMap, hash::Hash};

use super::Store;

struct Node<K, V> {
    key: K,
    val: Option<V>,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
}

impl<K, V> Node<K, V>
where
    K: Default + Copy + Clone,
{
    fn new(k: K, x: V) -> Self {
        Self {
            key: k,
            val: Some(x),
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        }
    }
    fn set_val(&mut self, val: V) {
        self.val.replace(val);
    }
}

impl<K, V> Default for Node<K, V>
where
    K: Default + Copy + Clone,
{
    fn default() -> Self {
        Self {
            key: K::default(),
            val: None,
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        }
    }
}

struct Dummy {}

impl<T> Store<T> for Dummy {
    fn store(&mut self, _data: &T) {}
}

static mut G_DUMMY: Dummy = Dummy {};

pub struct LRUCache<K, V> {
    head: *mut Node<K, V>,
    map: HashMap<K, *mut Node<K, V>>,
    backend: *mut dyn Store<V>,
    cap: usize,
    size: usize,
}

impl<K, V> LRUCache<K, V>
where
    K: Default + Eq + Hash + Copy + Clone,
{
    pub fn new(cap: usize) -> Self {
        let p = Box::into_raw(Box::new(Node::default()));
        unsafe {
            (*p).next = p;
            (*p).prev = p;
        }
        Self {
            head: p,
            map: HashMap::new(),
            backend: unsafe { std::ptr::addr_of_mut!(G_DUMMY) },
            cap,
            size: 0,
        }
    }

    pub fn set_backend(&mut self, b: *mut dyn Store<V>) {
        self.backend = b;
    }

    pub fn add(&mut self, key: K, val: V) {
        let e = self.map.get(&key);
        if e.is_none() {
            let node = Box::new(Node::new(key, val));
            let p = Box::into_raw(node);
            self.map.insert(key, p);
            self.push_back(p);
            self.size += 1;
        } else {
            let e = e.unwrap();
            unsafe {
                (*(*e)).set_val(val);
            }
        }

        if self.size > self.cap {
            let node = self.front();
            unsafe {
                self.size -= 1;
                self.map.remove(&(*node).key);
                self.remove_node(node);
                let tmp = (*node).val.take();
                (*self.backend).store(&tmp.unwrap());
                let _ = Box::from_raw(node);
            }
        }
    }

    pub fn get(&mut self, key: K) -> Option<&V> {
        if !self.map.contains_key(&key) {
            return None;
        }

        let tmp = self.map.get(&key).unwrap();
        unsafe {
            self.move_back(*tmp);
            (*(*tmp)).val.as_ref()
        }
    }

    pub fn flush(&mut self) {
        unsafe {
            let mut p = (*self.head).prev;
            while !p.eq(&self.head) {
                let prev = (*p).prev;
                // self.backend.store(&(*p).val);
                self.map.remove(&(*p).key);
                let tmp = (*p).val.take();
                (*self.backend).store(&tmp.unwrap());
                let _ = Box::from_raw(p);
                p = prev;
                self.size -= 1;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    fn push_back(&self, node: *mut Node<K, V>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*(*self.head).next).prev = node;
            (*node).prev = self.head;
            (*self.head).next = node;
        }
    }

    fn front(&self) -> *mut Node<K, V> {
        unsafe { (*self.head).prev }
    }

    fn remove_node(&self, node: *mut Node<K, V>) {
        unsafe {
            let prev = (*node).prev;
            let next = (*node).next;
            (*prev).next = next;
            (*next).prev = prev;
        }
    }

    fn move_back(&self, node: *mut Node<K, V>) {
        self.remove_node(node);
        self.push_back(node);
    }
}

#[cfg(test)]
mod test {
    use crate::cache::Store;

    use super::LRUCache;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct Backend {
        data: Rc<RefCell<Vec<i32>>>,
    }

    impl Store<i32> for Backend {
        fn store(&mut self, x: &i32) {
            self.data.borrow_mut().push(*x);
        }
    }

    #[test]
    fn test_lru() {
        let q = Rc::new(RefCell::new(Vec::new()));

        let mut backend = Backend { data: q.clone() };

        let cap = 3;
        let mut lru = LRUCache::new(cap);

        lru.set_backend(std::ptr::addr_of_mut!(backend));

        assert_eq!(lru.cap(), cap);

        lru.add(1, 1);
        lru.add(2, 2);
        lru.add(3, 3);
        lru.add(4, 4);

        assert_eq!(lru.len(), cap);

        assert_eq!(q.borrow().len(), 1);

        assert_eq!(lru.get(1), None);
        assert_eq!(lru.get(2), Some(&2));
        assert_eq!(lru.get(3), Some(&3));
        assert_eq!(lru.get(4), Some(&4));

        lru.flush();

        assert_eq!(q.borrow().len(), 4);
        assert_eq!(q.borrow()[0], 1);
        assert_eq!(q.borrow()[1], 2);
        assert_eq!(q.borrow()[2], 3);
        assert_eq!(q.borrow()[3], 4);

        assert_eq!(lru.len(), 0);
    }
}
