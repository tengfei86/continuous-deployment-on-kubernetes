package com.lgc.dspdm.msp.mainservice.client;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A POJO to model a person.
 * 
 * @author Wei Chiu
 *
 */
@XmlRootElement
public class Person {
	private String name;
	private int age;
	
	public Person() {}
	
	public Person(String name, int age) {
		this.name = name;
		this.age = age;
	}
	
	/**
	 * Gets person's name.
	 * @documentationExample John
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets person's name.
	 * @param name name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets person's age.
	 * @documentationExample 30
	 * @return age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * Sets person's age.
	 * @param age age.
	 */
	public void setAge(int age) {
		this.age = age;
	}
}
