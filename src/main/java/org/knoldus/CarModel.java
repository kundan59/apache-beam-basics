package org.knoldus;


import lombok.*;

/**
 * Car is a POJO class which contains information about the car.
 */
@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(force = true)
public final class CarModel {

    final String Name;
    final Double Miles_per_Gallon;
    final Long   Cylinders;
    final Double Displacement;
    final Long   Horsepower;
    final Long   Weight_in_lbs;
    final Double Acceleration;
    final String Year;
    final String Origin;
}
