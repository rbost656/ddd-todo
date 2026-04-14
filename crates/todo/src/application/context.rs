use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    pub meta: RequestMeta,
    pub actor: Option<ActorContext>,
    pub tenancy: Option<TenancyContext>,
    pub trace: TraceContext,
    pub exec: ExecutionContext,
}

impl RequestContext {
    pub fn new(source: RequestSource) -> Self {
        Self {
            meta: RequestMeta {
                request_id: "system".to_string(),
                correlation_id: None,
                causation_id: None,
                source,
            },
            actor: None,
            tenancy: None,
            trace: TraceContext::default(),
            exec: ExecutionContext::default(),
        }
    }

    pub fn system() -> Self {
        Self::new(RequestSource::System)
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.meta.request_id = request_id.into();
        self
    }

    pub fn with_actor(mut self, actor: ActorContext) -> Self {
        self.actor = Some(actor);
        self
    }

    pub fn with_tenancy(mut self, tenancy: TenancyContext) -> Self {
        self.tenancy = Some(tenancy);
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace.trace_id = Some(trace_id.into());
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.exec.idempotency_key = Some(key.into());
        self
    }

    pub fn with_deadline(mut self, deadline_at: SystemTime) -> Self {
        self.exec.deadline_at = Some(deadline_at);
        self
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::system()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMeta {
    pub request_id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub source: RequestSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestSource {
    System,
    Http,
    Grpc,
    Cli,
    Job,
    EventConsumer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorContext {
    pub user_id: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenancyContext {
    pub tenant_id: String,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExecutionContext {
    pub deadline_at: Option<SystemTime>,
    pub idempotency_key: Option<String>,
}
