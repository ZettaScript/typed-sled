use crate::*;

use std::marker::PhantomData;
use sled::{
    transaction::{ConflictableTransactionResult, TransactionResult},
    Result,
};

pub struct TransactionalTree<'a, K, V> {
    inner: &'a sled::transaction::TransactionalTree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<'a, K, V> TransactionalTree<'a, K, V> {
    pub fn insert(
        &self,
        key: &K,
        value: &V,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .insert(serialize(key), serialize(value))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn remove(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .remove(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn get(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }

    pub fn flush(&self) {
        self.inner.flush()
    }

    pub fn generate_id(&self) -> Result<u64> {
        self.inner.generate_id()
    }
}

pub trait TransactionalTrees<'a, E=()> {
    type Inner: sled::transaction::Transactional<E>;
	//type View;
	
	//fn from_sled_view(sled_transactional_tree: &Self::View) -> Self;
    fn from_sled_view(sled_transactional_tree: &'a <Self::Inner as sled::transaction::Transactional<E>>::View) -> Self;
}

impl<'a, K, V, E> TransactionalTrees<'a, E> for TransactionalTree<'a, K, V> {
    type Inner = &'a sled::Tree;
	//type View = sled::transaction::TransactionalTree;
	
	//fn from_sled_view(sled_transactional_tree: &Self::View) -> Self {
    fn from_sled_view(sled_transactional_tree: &'a <Self::Inner as sled::transaction::Transactional>::View) -> Self {
		Self {
			inner: sled_transactional_tree,
			_key: PhantomData,
			_value: PhantomData,
		}
	}
}

impl<'a, K1, V1, K2, V2, E> TransactionalTrees<'a, E> for (TransactionalTree<'a, K1, V1>, TransactionalTree<'a, K2, V2>) {
    type Inner = (&'a sled::Tree, &'a sled::Tree);
	//type View = (sled::transaction::TransactionalTree, sled::transaction::TransactionalTree);
	
	//fn from_sled_view(sled_transactional_tree: &Self::View) -> Self {
    fn from_sled_view(sled_transactional_tree: &'a <Self::Inner as sled::transaction::Transactional>::View) -> Self {
		(TransactionalTree {
			inner: &sled_transactional_tree.0,
			_key: PhantomData,
			_value: PhantomData,
		}, TransactionalTree {
			inner: &sled_transactional_tree.1,
			_key: PhantomData,
			_value: PhantomData,
		})
	}
}

/// A type that may be transacted on in sled transactions.
pub trait Transactional<E=()> {
	//type Inner: sled::transaction::Transactional<E>;
	
    /// An internal reference to an internal proxy type that
    /// mediates transactional reads and writes.
    //type View: TransactionalTrees<View = <Self::Inner as sled::transaction::Transactional>::View>;
    type View<'a>: TransactionalTrees<'a, E>;// where Self: 'a;

	//fn get_inner(&self) -> Self::Inner;
    fn get_inner(&self) -> <Self::View<'_> as TransactionalTrees<E>>::Inner;
	
    /// Runs a transaction, possibly retrying the passed-in closure if
    /// a concurrent conflict is detected that would cause a violation
    /// of serializability. This is the only trait method that
    /// you're most likely to use directly.
    fn transaction<'a: 'b, 'b, F, A>(&'a self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(Self::View<'b>) -> ConflictableTransactionResult<A, E>,
		//<Self::View as TransactionalTrees>::Inner: sled::transaction::Transactional<E>
    {
        //let c = |sled_view| f(Self::View::from_sled_view(sled_view));
        
        sled::transaction::Transactional::<E>::transaction(&self.get_inner(), move |sled_view| Self::c(f, sled_view))
    }
    
    fn c<'a: 'b, 'b, F, A>(f: F, sled_view: &'a <<Self::View<'b> as TransactionalTrees<'b, E>>::Inner as sled::transaction::Transactional<E>>::View) -> ConflictableTransactionResult<A, E>
    where
        F: Fn(Self::View<'b>) -> ConflictableTransactionResult<A, E>
    {
        f(Self::View::from_sled_view(sled_view))
    }
}

impl<K, V, E> Transactional<E> for &Tree<K, V> {
	type View<'a> = TransactionalTree<'a, K, V>;// where Self: 'a;
	//type Inner = &'a sled::Tree;

    fn get_inner(&self) -> <Self::View<'_> as TransactionalTrees<E>>::Inner {
		&self.inner
	}
}

impl<'a, K1, V1, K2, V2, E> Transactional<E> for (&'a Tree<K1, V1>, &'a Tree<K2, V2>) {
	type View<'b> = (TransactionalTree<'b, K1, V1>, TransactionalTree<'b, K2, V2>);// where Self: 'b;
	//type Inner = (&'a sled::Tree, &'a sled::Tree);

    fn get_inner(&self) -> <Self::View<'_> as TransactionalTrees<E>>::Inner {
		(&self.0.inner, &self.1.inner)
	}
}
