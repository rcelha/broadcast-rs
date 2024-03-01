#[derive(Clone, Debug)]
pub enum AdminCommands {
    Shutdown,
    Restart,
    Status,
    // Subscribe(client_id, channel)
    Subscribe(String, String),
    // Unsubscribe(client_id, channel)
    Unsubscribe(String, String),
}

pub struct AdminCommand {
    pub command: AdminCommands,
    pub underlying: String,
}

impl TryFrom<String> for AdminCommand {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<AdminCommand, Self::Error> {
        let mut parts = s.split(':');
        let command = match parts.next() {
            Some("shutdown") => AdminCommands::Shutdown,
            Some("restart") => AdminCommands::Restart,
            Some("status") => AdminCommands::Status,
            Some("subscribe") => {
                let user_id = parts
                    .next()
                    .ok_or(anyhow::anyhow!("No client_id"))?
                    .to_string();
                let channel = parts
                    .next()
                    .ok_or(anyhow::anyhow!("No channel"))?
                    .to_string();
                AdminCommands::Subscribe(user_id, channel)
            }
            Some("unsubscribe") => {
                let user_id = parts
                    .next()
                    .ok_or(anyhow::anyhow!("No client_id"))?
                    .to_string();
                let channel = parts
                    .next()
                    .ok_or(anyhow::anyhow!("No channel"))?
                    .to_string();
                AdminCommands::Unsubscribe(user_id, channel)
            }
            _ => return Err(anyhow::anyhow!("Invalid command"))?,
        };
        let underlying = s;
        Ok(AdminCommand {
            command,
            underlying,
        })
    }
}

impl From<AdminCommand> for String {
    fn from(val: AdminCommand) -> Self {
        val.underlying
    }
}
