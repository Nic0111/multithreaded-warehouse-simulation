use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Priority {
    Human,
    Bot,
}

impl Priority {
    pub fn from_bot_flag(bot: bool) -> Self {
        if bot { Self::Bot } else { Self::Human }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Bot => "bot",
        }
    }
}
