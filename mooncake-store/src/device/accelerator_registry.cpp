#include "device/accelerator_registry.h"

#include <atomic>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {
namespace device {

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX) || defined(USE_SUPA)
void EnsureCudaLikeAcceleratorDeviceLinked();
#endif
#if defined(USE_XPU)
void EnsureXpuAcceleratorDeviceLinked();
#endif

namespace {

void RegisterStaticAcceleratorDevice(const AcceleratorDevice& device);

class AcceleratorRegistryImpl final : public AcceleratorRegistry {
    using DeviceList = std::vector<const AcceleratorDevice*>;
    using DeviceListPtr = std::shared_ptr<const DeviceList>;

   public:
    std::span<const AcceleratorDevice* const> RegisteredDevices()
        const override {
        return std::span<const AcceleratorDevice* const>(
            registered_devices_.data(), registered_devices_.size());
    }

    RuntimeAccelerator RuntimeAccelerators(bool ensure = false) const override {
        // MC_STORE_DISABLE_ACCELERATORS: a store whose segments are host
        // memory sees GPUs only to allocate that memory (CUDA VMM host pools
        // need a GPU per NUMA node). Probing pointers there would create a
        // context holding device memory on a GPU a serving worker owns, so
        // every pointer is host memory instead.
        static const bool disabled = [] {
            const char* env = getenv("MC_STORE_DISABLE_ACCELERATORS");
            return env && *env && std::string(env) != "0";
        }();
        if (disabled) return RuntimeAccelerator();
        auto available_devices = std::atomic_load(&available_devices_);
        if (ShouldRefresh(ensure, available_devices)) {
            std::lock_guard<std::mutex> lock(refresh_mutex_);
            available_devices = std::atomic_load(&available_devices_);
            if (ShouldRefresh(ensure, available_devices)) {
                available_devices = BuildAvailableDevices();
                std::atomic_store(&available_devices_, available_devices);
            }
        }
        return RuntimeAccelerator(*available_devices);
    }

    const AcceleratorDevice* GetDevice(
        AcceleratorVendor vendor) const override {
        for (auto* device : registered_devices_) {
            if (device->Vendor() == vendor) return device;
        }
        return nullptr;
    }

   private:
    void Register(const AcceleratorDevice& device) {
        for (auto*& registered_device : registered_devices_) {
            if (registered_device->Vendor() == device.Vendor()) {
                registered_device = &device;
                return;
            }
        }
        registered_devices_.push_back(&device);
        std::atomic_store(&available_devices_, DeviceListPtr());
    }

    static bool ShouldRefresh(bool ensure,
                              const DeviceListPtr& available_devices) {
        return ensure || !available_devices || available_devices->empty();
    }

    DeviceListPtr BuildAvailableDevices() const {
        auto available_devices = std::make_shared<DeviceList>();
        for (auto* device : registered_devices_) {
            if (device->Available(true)) {
                available_devices->push_back(device);
            }
        }
        return available_devices;
    }

    DeviceList registered_devices_;
    mutable std::mutex refresh_mutex_;
    mutable std::shared_ptr<const DeviceList> available_devices_;

    friend void RegisterStaticAcceleratorDevice(
        const AcceleratorDevice& device);
};

AcceleratorRegistryImpl& MutableRegistry() {
    static AcceleratorRegistryImpl registry;
    return registry;
}

void RegisterStaticAcceleratorDevice(const AcceleratorDevice& device) {
    MutableRegistry().Register(device);
}

}  // namespace

const AcceleratorRegistry& GetAcceleratorRegistry() {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX) || defined(USE_SUPA)
    EnsureCudaLikeAcceleratorDeviceLinked();
#endif
#if defined(USE_XPU)
    EnsureXpuAcceleratorDeviceLinked();
#endif
    return MutableRegistry();
}

AcceleratorDeviceRegistrar::AcceleratorDeviceRegistrar(
    const AcceleratorDevice& device) {
    RegisterStaticAcceleratorDevice(device);
}

}  // namespace device
}  // namespace mooncake
