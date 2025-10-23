use std::{collections::HashMap, hash::Hash};

use super::Flusher;

struct Node<K, V> {
    key: Option<K>,
    val: Option<V>,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
}

impl<K, V> Node<K, V> {
    fn new(k: K, x: V) -> Self {
        Self {
            key: Some(k),
            val: Some(x),
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        }
    }
    fn set_val(&mut self, val: V) {
        self.val.replace(val);
    }
}

impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Self {
            key: None,
            val: None,
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        }
    }
}

struct Dummy {}

impl<K, V> Flusher<K, V> for Dummy {
    fn flush(&mut self, _key: K, _data: V) {}
}

static mut G_DUMMY: Dummy = Dummy {};

pub struct LRUCache<K: Eq + Hash + Clone, V> {
    head: *mut Node<K, V>,
    map: HashMap<K, *mut Node<K, V>>,
    backend: *mut dyn Flusher<K, V>,
    cap: usize,
    size: usize,
}

impl<K, V> LRUCache<K, V>
where
    K: Eq + Hash + Clone,
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
            backend: std::ptr::addr_of_mut!(G_DUMMY),
            cap,
            size: 0,
        }
    }

    #[allow(unused)]
    pub fn set_backend(&mut self, b: *mut dyn Flusher<K, V>) {
        self.backend = b;
    }

    pub fn add(&mut self, key: K, val: V) -> Option<&mut V> {
        let r = if let Some(e) = self.map.get(&key) {
            unsafe {
                (*(*e)).set_val(val);
            }
            self.move_back(*e);
            unsafe { (*(*e)).val.as_mut() }
        } else {
            let node = Box::new(Node::new(key.clone(), val));
            let p = Box::into_raw(node);
            self.map.insert(key, p);
            self.push_back(p);
            self.size += 1;
            unsafe { (*p).val.as_mut() }
        };

        if self.size > self.cap {
            let node = self.front();
            unsafe {
                self.size -= 1;
                let key = (*node).key.take().unwrap();
                self.map.remove(&key);
                self.remove_node(node);
                let val = (*node).val.take();
                (*self.backend).flush(key, val.unwrap());
                let _ = Box::from_raw(node);
            }
        }
        r
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if !self.map.contains_key(key) {
            return None;
        }

        let tmp = self.map.get(key).unwrap();
        self.move_back(*tmp);
        unsafe { (*(*tmp)).val.as_ref() }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.map.contains_key(key) {
            return None;
        }
        let tmp = self.map.get(key).unwrap();
        self.move_back(*tmp);
        unsafe { (*(*tmp)).val.as_mut() }
    }

    pub fn del(&mut self, key: &K) {
        if let Some(node) = self.map.remove(key) {
            self.remove_node(node);
            unsafe {
                let _ = Box::from_raw(node);
            }
            self.size -= 1;
        }
    }

    #[allow(unused)]
    pub fn flush(&mut self) {
        unsafe {
            while self.size > 0 {
                let node = self.front();
                self.remove_node(node);
                let key = (*node).key.take().unwrap();
                self.map.remove(&key);
                let val = (*node).val.take();
                (*self.backend).flush(key, val.unwrap());
                let _ = Box::from_raw(node);
                self.size -= 1;
            }
        }
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.size
    }

    #[allow(unused)]
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

impl<K, V> Drop for LRUCache<K, V>
where
    K: Eq + Hash + Clone,
{
    fn drop(&mut self) {
        let mut nodes = 1 + self.size; // including dummy head
        unsafe {
            let mut p = (*self.head).next;
            (*self.head).next = std::ptr::null_mut();
            while !p.is_null() {
                let prev = (*p).next;
                let _ = Box::from_raw(p);
                p = prev;
                nodes -= 1;
            }
        }
        assert_eq!(nodes, 0);
    }
}

#[cfg(test)]
mod test {
    use crate::cache::Flusher;

    use super::LRUCache;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct Backend {
        data: Rc<RefCell<Vec<i32>>>,
    }

    impl Flusher<i32, i32> for Backend {
        fn flush(&mut self, _k: i32, v: i32) {
            self.data.borrow_mut().push(v);
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

        lru.del(&4);
        assert_eq!(lru.len(), cap - 1);

        assert_eq!(q.borrow().len(), 1);

        assert_eq!(lru.get(&1), None);
        assert_eq!(lru.get(&2), Some(&2));
        assert_eq!(lru.get(&3), Some(&3));
        assert_eq!(lru.get(&4), None);

        lru.flush();

        assert_eq!(q.borrow().len(), cap);
        assert_eq!(q.borrow()[0], 1);
        assert_eq!(q.borrow()[1], 2);
        assert_eq!(q.borrow()[2], 3);

        assert_eq!(lru.len(), 0);

        lru.add(5, 5);
        assert_eq!(lru.len(), 1);
    }
}
