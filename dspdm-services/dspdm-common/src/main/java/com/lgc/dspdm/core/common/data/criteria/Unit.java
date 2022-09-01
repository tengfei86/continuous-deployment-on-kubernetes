package com.lgc.dspdm.core.common.data.criteria;

public class Unit {

	protected Unit() {
	}

	public Unit(Integer id, int baseTypeId, String name, String label) {
	//public Unit(Integer id, int baseTypeId, String name, String label, double a, double b, double c, double d, String source, String remark) {
		this.unit_id = id;
		this.base_type_id = baseTypeId;
		this.unit_name = name;
		this.unit_label = label;
//		this.unit_conv_factor_a = a;
//		this.unit_conv_factor_b = b;
//		this.unit_conv_factor_c = c;
//		this.unit_conv_factor_d = d;
//		this.source = source;
//		this.remark = remark;
	}

	private Integer unit_id;

	private int base_type_id;

	private String unit_name;

	private String unit_label;

//	private double unit_conv_factor_a;
//
//	private double unit_conv_factor_b;
//
//	private double unit_conv_factor_c;
//
//	private double unit_conv_factor_d;
//
//	private String source;
//
//	private String remark;

	public Integer getUnit_id() {
		return unit_id;
	}

	public int getBase_type_id() {
		return base_type_id;
	}

	public String getUnit_name() {
		return unit_name;
	}

	public String getUnit_label() {
		return unit_label;
	}

//	public double getUnit_conv_factor_a() {
//		return unit_conv_factor_a;
//	}
//
//	public double getUnit_conv_factor_b() {
//		return unit_conv_factor_b;
//	}
//
//	public double getUnit_conv_factor_c() {
//		return unit_conv_factor_c;
//	}
//
//	public double getUnit_conv_factor_d() {
//		return unit_conv_factor_d;
//	}
//
//	public String getSource() {
//		return source;
//	}
//
//	public String getRemark() {
//		return remark;
//	}

}
