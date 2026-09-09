//! Small SDK boundary used to exercise payment recovery without a live node.

use async_trait::async_trait;
use lexe::types::command::{
    CreateInvoiceRequest, CreateInvoiceResponse, GetPaymentRequest, GetPaymentResponse,
    GetUpdatedPaymentsRequest, GetUpdatedPaymentsResponse, ListPaymentsResponse, NodeInfo,
    PayInvoiceRequest,
};
use lexe::types::payment::{Payment, PaymentCreatedIndex, PaymentFilter};
use lexe::wallet::LexeWallet;

#[async_trait]
pub(crate) trait LexeClient: Send + Sync {
    async fn node_info(&self) -> anyhow::Result<NodeInfo>;
    async fn create_invoice(
        &self,
        req: CreateInvoiceRequest,
    ) -> anyhow::Result<CreateInvoiceResponse>;
    async fn pay_invoice(&self, req: PayInvoiceRequest) -> anyhow::Result<Payment>;
    async fn sync_payments(&self) -> anyhow::Result<()>;
    fn list_payments(
        &self,
        limit: usize,
        after: Option<&PaymentCreatedIndex>,
    ) -> anyhow::Result<ListPaymentsResponse>;
    async fn get_payment(&self, req: GetPaymentRequest) -> anyhow::Result<GetPaymentResponse>;
    async fn get_updated_payments(
        &self,
        req: GetUpdatedPaymentsRequest,
    ) -> anyhow::Result<GetUpdatedPaymentsResponse>;
}

#[async_trait]
impl LexeClient for LexeWallet {
    async fn node_info(&self) -> anyhow::Result<NodeInfo> {
        self.node_info().await
    }

    async fn create_invoice(
        &self,
        req: CreateInvoiceRequest,
    ) -> anyhow::Result<CreateInvoiceResponse> {
        self.create_invoice(req).await
    }

    async fn pay_invoice(&self, req: PayInvoiceRequest) -> anyhow::Result<Payment> {
        self.pay_invoice(req).await
    }

    async fn sync_payments(&self) -> anyhow::Result<()> {
        self.sync_payments().await.map(|_| ())
    }

    fn list_payments(
        &self,
        limit: usize,
        after: Option<&PaymentCreatedIndex>,
    ) -> anyhow::Result<ListPaymentsResponse> {
        self.list_payments(&PaymentFilter::All, None, Some(limit), after)
    }

    async fn get_payment(&self, req: GetPaymentRequest) -> anyhow::Result<GetPaymentResponse> {
        self.get_payment(req).await
    }

    async fn get_updated_payments(
        &self,
        req: GetUpdatedPaymentsRequest,
    ) -> anyhow::Result<GetUpdatedPaymentsResponse> {
        self.get_updated_payments(req).await
    }
}
