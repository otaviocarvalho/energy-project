package energy_stream

case class Measurement(id: Int,
                       timestamp: Int,
                       value: Float,
                       property: Int,
                       plug_id: Int,
                       household_id: Int,
                       house_id: Int,
                       hhp_id: String)

case class PlugMeasurement(house_id: Int,
                           household_id: Int,
                           plug_id: Int,
                           timestamp: Int,
                           value: Double,
                           median: Double,
                           hhp_id: String)

case class HouseMeasurement(house_id: Int,
                            timestamp: Int,
                            value: Double,
                            median: Double,
                            hhp_id: String)

case class HousePrediction(house_id: Int,
                           timestamp: Int,
                           predicted_load: Double)

case class PlugPrediction(house_id: Int,
                          household_id: Int,
                          plug_id: Int,
                          timestamp: Int,
                          predicted_load: Double)
