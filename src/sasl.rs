//! Mock SASL acceptor for the Azure Service Bus emulator.
//!
//! Advertises ANONYMOUS, PLAIN, MSSBCBS, and EXTERNAL mechanisms and
//! accepts any credentials. Actual authentication happens at the CBS
//! (Claims-Based Security) layer after the AMQP connection is established.

use fe2o3_amqp::acceptor::sasl_acceptor::SaslServerFrame;
use fe2o3_amqp::acceptor::SaslAcceptor;
use fe2o3_amqp_types::primitives::{Array, Symbol};
use fe2o3_amqp_types::sasl::{SaslCode, SaslInit, SaslOutcome, SaslResponse};

/// SASL mechanisms advertised to clients.
const MECHANISMS: &[&str] = &["ANONYMOUS", "PLAIN", "MSSBCBS", "EXTERNAL"];

/// A mock SASL acceptor that accepts any credentials.
///
/// The Azure Service Bus SDK negotiates SASL using the MSSBCBS mechanism
/// before performing CBS token authentication over AMQP links. This acceptor
/// simply allows any mechanism and credentials through, deferring real
/// authentication to the CBS layer.
#[derive(Debug, Clone)]
pub struct MockSaslAcceptor;

impl SaslAcceptor for MockSaslAcceptor {
    fn mechanisms(&self) -> Array<Symbol> {
        Array::from(
            MECHANISMS
                .iter()
                .map(|m| Symbol::from(*m))
                .collect::<Vec<_>>(),
        )
    }

    fn on_init(&mut self, _init: SaslInit) -> SaslServerFrame {
        SaslServerFrame::Outcome(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: None,
        })
    }

    fn on_response(&mut self, _response: SaslResponse) -> SaslServerFrame {
        SaslServerFrame::Outcome(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mechanisms_lists_all_four() {
        let acceptor = MockSaslAcceptor;
        let mechs = acceptor.mechanisms();
        let names: Vec<&str> = mechs.0.iter().map(|s| s.as_str()).collect();
        assert_eq!(names, vec!["ANONYMOUS", "PLAIN", "MSSBCBS", "EXTERNAL"]);
    }

    #[test]
    fn test_on_init_always_ok() {
        let mut acceptor = MockSaslAcceptor;
        let init = SaslInit {
            mechanism: Symbol::from("MSSBCBS"),
            initial_response: None,
            hostname: Some("localhost".to_string()),
        };
        match acceptor.on_init(init) {
            SaslServerFrame::Outcome(outcome) => {
                assert_eq!(outcome.code, SaslCode::Ok);
            }
            _ => panic!("Expected Outcome, got Challenge"),
        }
    }

    #[test]
    fn test_on_init_accepts_any_mechanism() {
        let mut acceptor = MockSaslAcceptor;
        for mech in &["ANONYMOUS", "PLAIN", "MSSBCBS", "EXTERNAL", "UNKNOWN"] {
            let init = SaslInit {
                mechanism: Symbol::from(*mech),
                initial_response: None,
                hostname: None,
            };
            match acceptor.on_init(init) {
                SaslServerFrame::Outcome(outcome) => {
                    assert_eq!(outcome.code, SaslCode::Ok);
                }
                _ => panic!("Expected Outcome for mechanism {}", mech),
            }
        }
    }

    #[test]
    fn test_on_response_always_ok() {
        let mut acceptor = MockSaslAcceptor;
        let response = SaslResponse {
            response: fe2o3_amqp_types::primitives::Binary::from(vec![0u8; 4]),
        };
        match acceptor.on_response(response) {
            SaslServerFrame::Outcome(outcome) => {
                assert_eq!(outcome.code, SaslCode::Ok);
            }
            _ => panic!("Expected Outcome, got Challenge"),
        }
    }

    #[test]
    fn test_acceptor_is_clone() {
        let acceptor = MockSaslAcceptor;
        let _cloned = acceptor.clone();
    }
}
