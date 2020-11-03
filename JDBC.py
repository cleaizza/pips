from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = [
    'WriteToJdbc',
    'ReadFromJdbc',
]

def default_io_expansion_service():
  return BeamJarExpansionService('sdks:java:io:expansion-service:shadowJar')


WriteToJdbcSchema = typing.NamedTuple(
    'WriteToJdbcSchema',
    [
        ('driver_class_name', unicode),
        ('jdbc_url', unicode),
        ('username', unicode),
        ('password', unicode),
        ('connection_properties', typing.Optional[unicode]),
        ('connection_init_sqls', typing.Optional[typing.List[unicode]]),
        ('statement', unicode),
    ],
)


[docs]class WriteToJdbc(ExternalTransform):

    ExampleRow = typing.NamedTuple('ExampleRow',
                                   [('id', int), ('name', unicode)])
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create([ExampleRow(11, 'abc', 'abc', '2020-01-01', 'a', 1, 'abc', 12)])
              .with_output_types(ExampleRow)
          | 'Write to jdbc' >> WriteToJdbc(
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/34.67.226.28',
              username='poc360',
              password='HDISENHA',
              statement='INSERT INTO poc360.tbCliente VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
          ))

  URN = 'beam:external:java:jdbc:write:v1'

  def __init__(
      self,
      driver_class_name,
      jdbc_url,
      username,
      password,
      statement,
      connection_properties=None,
      connection_init_sqls=None,
      expansion_service=None,
  ):

    super(WriteToJdbc, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToJdbcSchema(
                driver_class_name=driver_class_name,
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                statement=statement,
                connection_properties=connection_properties,
                connection_init_sqls=connection_init_sqls,
            ),
        ),
        expansion_service or default_io_expansion_service(),
    )
	
	

