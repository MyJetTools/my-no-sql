use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSerializerMetadata, TcpSerializerMetadataFactory, TcpSocketSerializer, TcpWriteBuffer,
};

use crate::MyNoSqlTcpContract;

pub struct MyNoSqlReaderTcpSerializer;

impl MyNoSqlReaderTcpSerializer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MyNoSqlReaderTcpSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TcpSocketSerializer<MyNoSqlTcpContract, ()> for MyNoSqlReaderTcpSerializer {
    fn serialize(&self, out: &mut impl TcpWriteBuffer, contract: &MyNoSqlTcpContract, _: &()) {
        contract.serialize(out)
    }

    fn get_ping(&self) -> MyNoSqlTcpContract {
        MyNoSqlTcpContract::Ping
    }

    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
        _: &(),
    ) -> Result<MyNoSqlTcpContract, ReadingTcpContractFail> {
        MyNoSqlTcpContract::deserialize(socket_reader).await
    }
}

impl TcpSerializerMetadata<MyNoSqlTcpContract> for () {
    fn is_tcp_contract_related_to_metadata(&self, _: &MyNoSqlTcpContract) -> bool {
        false
    }
    fn apply_tcp_contract(&mut self, _: &MyNoSqlTcpContract) {}
}

pub struct MyNoSqlTcpSerializerFactory;

#[async_trait::async_trait]
impl TcpSerializerMetadataFactory<MyNoSqlTcpContract, ()> for MyNoSqlTcpSerializerFactory {
    async fn create(&self) -> () {
        ()
    }
}
