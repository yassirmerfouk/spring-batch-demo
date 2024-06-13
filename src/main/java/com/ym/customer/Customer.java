package com.ym.customer;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@NoArgsConstructor @AllArgsConstructor @Setter @Getter @Builder
public class Customer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String firstName;
    private String lastName;
    private String phone;
    private String gender;
    private LocalDate birthDate;

    @Transient
    private String strBirthDate;

    public static String[] fields(){
        return new String[]{"firstName", "lastName", "phone", "gender", "strBirthDate"};
    }
}
