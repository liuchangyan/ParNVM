##Dev Notes##
TO LOOKUP:
 [] How Sto implements the item version checking


Issues: 
**Problem Statement:**
A trait object TObject which needs to be used safely across threads.Since it represents a data object which is globally accessible.  So potentially, it needs to be like something as a smart pointer. 

There are structs which implement TObject, such as TBox... Data are initizialied as those sub structs, but passed around to a Transaction manager as a TObject. 

**Pattern Doesn't work well**:
First try implementation is to have a `_TObject` that is associated with implementations, have `TBox` implements `_TObject`, and make `TObject` as a type alias to `Rc<RefCell<_TObject>>`
However, the complier produces type mismatched error, not able to referring `Rc<RefCell<TBox>>` to `Rc<RefCell<_TObject>>`. 

**Walkaround**
The current solution used is to add some syntax on explicit casting. 
```rust
    let tb1 = TBox<u32>::new(1) as TObject<u32>;
    tx.read(&tb1);
```
However, ideal situation should be without the `as` part and what follows it. 

**Alternatives but I don't think work**
- Associated Trait: Strictly speaking, the use case is different form the typical usage of the associated trait shown online, where the target type is just another type, rather than a trait. 
- Deref: hmmm, its interface is limited, and it is more suitable for reference conversion. But in this case, there is no `&T -> &U` specifically. 
