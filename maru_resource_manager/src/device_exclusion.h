#pragma once

#include <set>
#include <string>

namespace maru {

/// Normalize "daxN.M" or "/dev/daxN.M" to "daxN.M". False for anything else.
bool normalizeDaxName(const std::string &in, std::string &out);

/// Devices the RM must never open (e.g. handed to ROOF), persisted in
/// <stateDir>/excluded_devices and managed by maru-rm-ctl. A missing file
/// means none; a malformed one is -EINVAL, never an empty list.
int loadExcludedDevices(const std::string &stateDir, std::set<std::string> &out);
int saveExcludedDevices(const std::string &stateDir,
                        const std::set<std::string> &names);

} // namespace maru
