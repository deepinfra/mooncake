if (CUDAToolkit_FOUND)
    file(GLOB TENT_PLATFORM_CUDA_SOURCES "*.cpp")
    # The batched copy kernel needs nvcc; without it copyBatch keeps using
    # the copy engines only.
    if (NOT CMAKE_CUDA_COMPILER AND NOT DEFINED ENV{CUDACXX}
        AND CUDAToolkit_NVCC_EXECUTABLE)
        set(CMAKE_CUDA_COMPILER ${CUDAToolkit_NVCC_EXECUTABLE})
    endif()
    include(CheckLanguage)
    check_language(CUDA)
    if (CMAKE_CUDA_COMPILER)
        if (NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
            set(CMAKE_CUDA_ARCHITECTURES 80 90 100)
            if (CUDAToolkit_VERSION VERSION_GREATER_EQUAL 12.9)
                list(APPEND CMAKE_CUDA_ARCHITECTURES 103)
            endif()
        endif()
        enable_language(CUDA)
        list(APPEND TENT_PLATFORM_CUDA_SOURCES batched_copy.cu)
    endif()
    add_library(platform_cuda STATIC ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver)
    if (CMAKE_CUDA_COMPILER)
        target_compile_definitions(platform_cuda PRIVATE TENT_BATCHED_COPY_KERNEL)
        set_target_properties(platform_cuda PROPERTIES
            POSITION_INDEPENDENT_CODE ON)
    endif()
endif()
