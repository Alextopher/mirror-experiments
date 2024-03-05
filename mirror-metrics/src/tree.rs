use std::path::{Path, PathBuf};

use crate::nginx::Metric;

#[derive(Debug)]
pub struct Tree<'a> {
    pub root: Node<'a>,
    pub size: usize,
}

#[derive(Debug, Clone)]
pub struct Node<'a> {
    pub children: Vec<Node<'a>>,
    pub component: &'a str,
    pub data: Metric,
}

impl<'a> Node<'a> {
    // Merge 2 nodes
    pub fn merge(&mut self, other: &Node<'a>) {
        self.data += other.data;

        let mut other_children = other.children.clone();
        for child in &mut self.children {
            if let Some(other_child) = other_children
                .iter()
                .find(|c| c.component == child.component)
            {
                child.merge(other_child);
                other_children.retain(|c| c.component != child.component);
            }
        }

        self.children.extend(other_children);
    }
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

    // Produces the union of 2 trees
    pub fn union(&mut self, other: &Tree<'a>) {
        // Merge the root nodes
        self.root.merge(&other.root);
    }

    pub fn serialize(&self, writer: &mut impl std::io::Write) -> anyhow::Result<()> {
        let mut stack = vec![(&self.root, PathBuf::new())];

        while let Some((node, path)) = stack.pop() {
            if node.children.is_empty() {
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

    pub fn deserialize(file: &'a str) -> anyhow::Result<Self> {
        let mut tree = Tree::new();

        file.lines()
            .filter_map(|l| {
                let mut parts = l.split_whitespace();

                let path = Path::new(parts.next()?);
                let requests = parts.next()?.parse().ok()?;
                let bytes_received = parts.next()?.parse().ok()?;
                let bytes_sent = parts.next()?.parse().ok()?;

                Some((
                    path,
                    Metric {
                        requests,
                        bytes_sent,
                        bytes_received,
                    },
                ))
            })
            .for_each(|(path, metric)| tree.insert(path, metric));

        Ok(tree)
    }
}
