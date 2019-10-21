package models

import "github.com/jinzhu/gorm"

type Copy struct {
	Id      int     `gorm:"column:id"`
	FromMt4 int     `gorm:"column:from_mt4"`
	ToMt4   int     `gorm:"column:to_mt4"`
	Source  int     `gorm:"column:source"`
	Amount  float64 `gorm:"column:amount"`
}

func (Copy) TableName() string {
	return "copy"
}

func (Copy) SchemaName() string {
	return "tiger"
}

func (c *Copy) Create(db *gorm.DB) error {
	return db.Create(c).Error
}

func (c *Copy) Update(db *gorm.DB) error {
	return db.Update(c).Error
}

func (c *Copy) Delete(db *gorm.DB) error  {
	return db.Delete(c).Error
}