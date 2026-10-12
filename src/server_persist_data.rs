use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use api::{
    gui::CameraDetails,
    shared::{CameraStatus, ConnId, Deviation}
};
use crate::server::UniqueCameraID;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShortCameraDetails {
    pub name: String,
    pub warn_below_threshold: Deviation,
    pub alert_below_threshold: Deviation,
    pub warn_above_threshold: Deviation,
    pub alert_above_threshold: Deviation,
}

impl From<CameraDetails> for ShortCameraDetails {
    fn from(details: CameraDetails) -> Self {
        ShortCameraDetails {
            name: details.name,
            warn_below_threshold: details.warn_below_threshold,
            alert_below_threshold: details.alert_below_threshold,
            warn_above_threshold: details.warn_above_threshold,
            alert_above_threshold: details.alert_above_threshold,
        }
    }
}

impl From<ShortCameraDetails> for CameraDetails {
    fn from(details: ShortCameraDetails) -> Self {
        CameraDetails {
            name: details.name,
            state: CameraStatus::Disconnected,
            warn_below_threshold: details.warn_below_threshold,
            alert_below_threshold: details.alert_below_threshold,
            warn_above_threshold: details.warn_above_threshold,
            alert_above_threshold: details.alert_above_threshold,
        }
    }
}

impl From<UniqueCameraID> for String {
    fn from(id: UniqueCameraID) -> Self {
        serde_json::to_string(&id).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentServerData {
    pub edge_names: HashMap<ConnId, String>,

    pub cameras: HashMap<String, ShortCameraDetails>,

    pub default_warn_threshold: Deviation,

    pub default_alert_threshold: Deviation,

    pub default_warn_above_threshold: Deviation,

    pub default_alert_above_threshold: Deviation,
}

impl Default for PersistentServerData {
    fn default() -> Self {
        PersistentServerData {
            edge_names: HashMap::new(),
            cameras: HashMap::new(),
            default_warn_threshold: 0.9,
            default_alert_threshold: 0.8,
            default_warn_above_threshold: 1.1,
            default_alert_above_threshold: 1.2,
        }
    }
}