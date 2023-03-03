// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use crate::type_map::TypeMap;

static REGISTRIES: Lazy<Arc<Mutex<HashMap<String, Registry>>>> = Lazy::new(Default::default);

/// A registry for singleton objects.
#[derive(Debug, Clone)]
pub struct Registry(Arc<Mutex<TypeMap>>);

impl Registry {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(TypeMap::default())))
    }

    pub fn register<T>(&mut self, instance: T)
    where T: Any + Clone + Send + Sync + 'static {
        let mut instances = self.0.lock().expect("The lock should never be poisoned.");
        instances.insert(instance);
    }

    pub fn get<T: Any + Clone + Send + Sync + 'static>(&self) -> Option<T>
    where T: Any + Clone + Send + Sync + 'static {
        let instances = self.0.lock().expect("The lock should never be poisoned.");
        instances.get().cloned()
    }

    pub fn for_namespace(namespace_name: &str) -> Self {
        let mut registries = REGISTRIES
            .lock()
            .expect("The lock should never be poisoned.");

        if let Some(registry) = registries.get(namespace_name) {
            return registry.clone();
        }
        let registry = Registry::new();
        registries.insert(namespace_name.to_string(), registry.clone());
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct Foo(usize);

    #[tokio::test]
    async fn test_registry() {
        let mut registry = Registry::new();
        assert!(registry.get::<Foo>().is_none());

        registry.register(Foo(42));

        let foo: Foo = registry.get().unwrap();
        assert_eq!(foo.0, 42);
    }

    #[tokio::test]
    async fn test_registries() {
        let mut registry = Registry::for_namespace("node-1");
        assert!(registry.get::<Foo>().is_none());

        registry.register(Foo(42));

        let foo: Foo = registry.get().unwrap();
        assert_eq!(foo.0, 42);

        let registry = Registry::for_namespace("node-1");
        let foo: Foo = registry.get().unwrap();
        assert_eq!(foo.0, 42);

        let registry = Registry::for_namespace("node-2");
        assert!(registry.get::<Foo>().is_none());
    }
}
