from django.db import models

class Feacsg(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'feacsg'

class Feadiv(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'feadiv'

class Feagsp(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'feagsp'

class L1Pinda(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'l1pinda'

class Lam2Csg(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lam2csg'

class Lw01Pinda(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lw01pinda'

class Puccsg(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'puccsg'

class Shrcsg(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'shrcsg'

class Shrgsp(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    color = models.CharField(max_length=25, blank=True, null=True)
    startdate = models.DateTimeField(primary_key=True, blank=True)
    enddate = models.DateTimeField(blank=True, null=True)
    cameras = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'shrgsp'