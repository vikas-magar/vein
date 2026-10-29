use super::*;

pub(super) fn validate(metadata: &OperatorMetadata, modes: &[ExtensionInputMode], phase: PipelinePhase) -> Result<()> {
    if modes.len() != metadata.children.len() {
        return Err(VeinError::planning("operator phase modes do not cover its child ports", None));
    }
    let valid = match phase {
        PipelinePhase::Source => metadata.role == OperatorRole::Source
            || (metadata.role == OperatorRole::Transform && modes.iter().all(|mode| *mode == ExtensionInputMode::Blocking)),
        PipelinePhase::Transform(port) => metadata.role == OperatorRole::Transform
            && modes.get(port.index()) == Some(&ExtensionInputMode::Streaming),
        PipelinePhase::Sink(port) => match metadata.role {
            OperatorRole::Source => false,
            OperatorRole::Transform => modes.get(port.index()) == Some(&ExtensionInputMode::Blocking),
            OperatorRole::Sink => modes.get(port.index()).is_some(),
        },
        PipelinePhase::Finalize => metadata.role == OperatorRole::Sink
            && modes.iter().all(|mode| *mode == ExtensionInputMode::Blocking),
    };
    if !valid {
        return Err(VeinError::planning(format!("operator does not support pipeline phase {phase:?}"), None));
    }
    Ok(())
}
