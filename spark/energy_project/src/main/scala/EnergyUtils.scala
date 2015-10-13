package energy_stream

case class Measurement(id: Int,
                       timestamp: Int,
                       value: Float,
                       property: Int,
                       plug_id: Int,
                       household_id: Int,
                       house_id: Int
                       //hhp_id: String
                        )

case class PlugMeasurement(house_id: Int,
                           household_id: Int,
                           plug_id: Int,
                           value: Double)
