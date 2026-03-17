Scans算子，预测的是延时ms
$$Latency_{Total} = 4.38 \times 10^{-5} \cdot (Rows \cdot RowSize) + 1.51 \times 10^{-4} \cdot (Rows \cdot \log_2(RowSize)) + 315.2$$

Scan列存算子，
Lantency = -5.23e-6   * rows * rowSize + 6.57e-4 * rows * log2(rowSize) - 6609.80

IVF索引算子，
$$Cost_{coarse} = nlist \times dim \times 1.0^{-3}$$
$$Cost_{fine} = \left( nprobe \times \frac{N_{total}}{nlist} \right) \times dim \times 1.0^{-3}$$

hashjoin算子，
Latency = buildRows * buildRowSize * 4.38e-5 + buildRows * buildFilters * 1.73e-3 + buildRows * nKeys * 1.73e-3 + buildRows * buildRowSize * 1.05e-4 + buildRows * 1.73e-3 + probeRows * probeFilters * 1.73e-3 + probeRows * nKeys * 1.73e-3 + probeRows * probeRowSize * 1.05e-4 + probeRows * 1.73e-3