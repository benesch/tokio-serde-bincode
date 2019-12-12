//! `Stream` and `Sink` adaptors for serializing and deserializing values using
//! Bincode.
//!
//! This crate provides adaptors for going from a stream or sink of buffers
//! ([`Bytes`]) to a stream or sink of values by performing Bincode encoding or
//! decoding. It is expected that each yielded buffer contains a single
//! serialized Bincode value. The specific strategy by which this is done is left
//! up to the user. One option is to use using [`length_delimited`] from
//! [tokio-io].
//!
//! [`Bytes`]: https://docs.rs/bytes/0.4/bytes/struct.Bytes.html
//! [`length_delimited`]: http://alexcrichton.com/tokio-io/tokio_io/codec/length_delimited/index.html
//! [tokio-io]: http://github.com/alexcrichton/tokio-io
//! [examples]: https://github.com/carllerche/tokio-serde-json/tree/master/examples

extern crate bincode;
extern crate bytes;
extern crate futures;
extern crate serde;
extern crate tokio_serde;

use bincode::Error;
use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, TryStream};
use futures::sink::Sink;
use serde::{Deserialize, Serialize};
use tokio_serde::{Deserializer, Framed, Serializer};

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Adapts a stream of Bincode encoded buffers to a stream of values by
/// deserializing them.
///
/// `ReadBincode` implements `Stream` by polling the inner buffer stream and
/// deserializing the buffer as Bincode. It expects that each yielded buffer
/// represents a single Bincode value and does not contain any extra trailing
/// bytes.
pub struct ReadBincode<T, U> {
    inner: Framed<T, U, (), Bincode<U>>,
}

/// Adapts a buffer sink to a value sink by serializing the values as Bincode.
///
/// `WriteBincode` implements `Sink` by serializing the submitted values to a
/// buffer. The buffer is then sent to the inner stream, which is responsible
/// for handling framing on the wire.
pub struct WriteBincode<T: Sink<Bytes>, U> {
    inner: Framed<T, (), U, Bincode<U>>,
}

struct Bincode<T> {
    ghost: PhantomData<T>,
}

impl<T, U> ReadBincode<T, U>
where
    T: TryStream<Ok = BytesMut>,
    T::Error: From<Error>,
    U: for<'de> Deserialize<'de>,
{
    /// Creates a new `ReadBincode` with the given buffer stream.
    pub fn new(inner: T) -> ReadBincode<T, U> {
        let bincode = Bincode { ghost: PhantomData };
        ReadBincode {
            inner: Framed::new(inner, bincode),
        }
    }
}

impl<T, U> ReadBincode<T, U> {
    /// Returns a reference to the underlying stream wrapped by `ReadBincode`.
    ///
    /// Note that care should be taken to not tamper with the underlying stream
    /// of data coming in as it may corrupt the stream of frames otherwise
    /// being worked with.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying stream wrapped by
    /// `ReadBincode`.
    ///
    /// Note that care should be taken to not tamper with the underlying stream
    /// of data coming in as it may corrupt the stream of frames otherwise
    /// being worked with.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Consumes the `ReadBincode`, returning its underlying stream.
    ///
    /// Note that care should be taken to not tamper with the underlying stream
    /// of data coming in as it may corrupt the stream of frames otherwise being
    /// worked with.
    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T, U> Stream for ReadBincode<T, U>
where
    T: TryStream<Ok = BytesMut>,
    T::Error: From<Error>,
    U: for<'de> Deserialize<'de>,
{
    type Item = Result<U, <T as TryStream>::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|x| &mut x.inner) }.poll_next(cx)
    }
}

impl<T, U> WriteBincode<T, U>
where
    T: Sink<Bytes>,
    T::Error: From<Error>,
    U: Serialize,
{
    /// Creates a new `WriteBincode` with the given buffer sink.
    pub fn new(inner: T) -> WriteBincode<T, U> {
        let json = Bincode { ghost: PhantomData };
        WriteBincode {
            inner: Framed::new(inner, json),
        }
    }
}

impl<T: Sink<Bytes>, U> WriteBincode<T, U> {
    /// Returns a reference to the underlying sink wrapped by `WriteBincode`.
    ///
    /// Note that care should be taken to not tamper with the underlying sink as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying sink wrapped by
    /// `WriteBincode`.
    ///
    /// Note that care should be taken to not tamper with the underlying sink as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Consumes the `WriteBincode`, returning its underlying sink.
    ///
    /// Note that care should be taken to not tamper with the underlying sink as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    fn get_pin(self: Pin<&mut Self>) -> Pin<&mut Framed<T, (), U, Bincode<U>>> {
        unsafe { self.map_unchecked_mut(|x| &mut x.inner) }
    }
}

impl<T, U> Sink<U> for WriteBincode<T, U>
where
    T: Sink<Bytes>,
    T::Error: From<Error>,
    U: Serialize,
{
    type Error = <T as Sink<Bytes>>::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.get_pin().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: U) -> Result<(), Self::Error> {
        self.get_pin().start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.get_pin().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.get_pin().poll_close(cx)
    }
}

impl<T> Deserializer<T> for Bincode<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Error = Error;

    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<T, Error> {
        bincode::deserialize(src)
    }
}

impl<T: Serialize> Serializer<T> for Bincode<T> {
    type Error = Error;

    fn serialize(self: Pin<&mut Self>, item: &T) -> Result<Bytes, Self::Error> {
        bincode::serialize(item).map(Into::into)
    }
}
