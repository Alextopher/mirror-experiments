use std::path::{Path, PathBuf};

use crate::nginx::Metric;

#[derive(Debug)]
pub struct Tree<'a> {
    pub root: Node<'a>,
    pub size: usize,
}

// Node
#[derive(Debug, Clone)]
pub struct Node<'a> {
    pub children: Vec<Node<'a>>,
    pub component: &'a str,
    pub data: Metric,
}

impl<'a> Extend<(&'a Path, Metric)> for Tree<'a> {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (&'a Path, Metric)>,
    {
        for (path, metric) in iter {
            self.insert(path.as_ref(), metric);
        }
    }
}

impl<'a> FromIterator<(&'a Path, Metric)> for Tree<'a> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a Path, Metric)>,
    {
        let mut tree = Tree::new();
        tree.extend(iter);
        tree
    }
}

impl<'a> Tree<'a> {
    pub fn new() -> Self {
        let root = Node {
            children: Vec::new(),
            data: Metric::default(),
            component: "",
        };

        Tree { root, size: 0 }
    }

    pub fn insert(&mut self, path: &'a Path, metric: Metric) {
        let mut current = &mut self.root;
        for component in path.components() {
            current.data += metric;

            let component_str = component.as_os_str().to_str().unwrap();

            // If there is a matching child, use it
            let mut child_index = None;
            for (i, child) in current.children.iter().enumerate() {
                if child.component == component_str {
                    child_index = Some(i);
                    break;
                }
            }

            if let Some(index) = child_index {
                current = &mut current.children[index];
                continue;
            }

            // Otherwise, create a new node
            let new_node = Node {
                children: Vec::new(),
                data: Metric::default(),
                component: component_str,
            };

            current.children.push(new_node);
            current = current.children.last_mut().unwrap();

            self.size += 1;
        }

        current.data += metric;
    }

    pub fn serialize(&self, writer: &mut impl std::io::Write) -> anyhow::Result<()> {
        let mut stack = vec![(&self.root, PathBuf::new())];

        while let Some((node, path)) = stack.pop() {
            if node.children.is_empty() && node.data.requests > 1 {
                writeln!(
                    writer,
                    "{} {} {} {}",
                    path.display(),
                    node.data.requests,
                    node.data.bytes_received,
                    node.data.bytes_sent
                )?;
                continue;
            }

            // Sort the children so that the output is deterministic
            // Because of reference issues we keep an index and sort the indices
            let mut children = node.children.iter().collect::<Vec<_>>();
            children.sort_by_key(|child| child.component);

            for child in children {
                let mut path = path.clone();
                path.push(child.component);
                stack.push((child, path));
            }
        }

        Ok(())
    }
}
