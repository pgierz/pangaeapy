import shutil
from io import BytesIO
from zipfile import ZipFile

from pangaeapy.exporter.pan_exporter import PanExporter
import os
import json
class PanFrictionlessExporter(PanExporter):
    def get_csv(self):
        csv =''
        #print(self.pandataset.data.head())
        try:
            csv = self.pandataset.data.to_csv(os.path.join(self.filelocation,+'data.csv'), index=False)
        except Exception as e:
            self.logging.append({'ERROR': f'Frictionless CSV creation failed: {str(e)}'})

        return csv

    def create_tableschema_json(self):
        schema = {'fields':[]}
        typeconv={'numeric':'number'}
        for k, p in self.pandataset.params.items():
            #print(k, p.name, p.shortName)
            pantype = p.type
            if pantype in typeconv:
                pantype = typeconv.get(pantype)
            field = {'name': k, 'title': p.name, 'type': pantype}
            if panunit := p.unit:
                field['unit'] = panunit
            if pancomment := p.comment:
                field['description'] = pancomment
            schema['fields'].append(field)
        return schema

    def get_package_json(self):
        package = {'profile':'tabular-data-package'}
        panauthors = []
        try:
            panauthors.extend(
                {
                    'title': f'{author.firstname} {author.lastname}',
                    'role': 'author',
                }
                for author in self.pandataset.authors
            )

            table_schema = self.create_tableschema_json()

            resources = [
                {
                    'profile': 'tabular-data-resource',
                    'path': f'{self.pandataset.id}_data.csv',
                    'schema': table_schema,
                }
            ]


            package['name'] = f'{self.pandataset.id}_metadata'
            package['id'] = self.pandataset.doi
            package['title'] = self.pandataset.title
            if self.pandataset.abstract:
                package['description'] = self.pandataset.abstract
            package['created'] = self.pandataset.date
            package['contributors'] = panauthors
            package['licenses'] = [{'path':self.pandataset.licence.URI, 'name':self.pandataset.licence.label, 'title':self.pandataset.licence.name}]
            package['resources'] =resources
        except Exception as e:
            self.logging.append({'ERROR': f'Frictionless JSON creation failed: {str(e)}'})
        return json.dump(package)

    def create(self):
        in_memory_zip = False
        ret = False
        if self.pandataset.isParent:
            self.logging.append({'ERROR':'Cannot export a parent type dataset to frictionless'})
        elif self.pandataset.loginstatus == 'unrestricted':
            try:
                in_memory_zip = BytesIO()
                zip_file = ZipFile(in_memory_zip, 'w')
                package = self.get_package_json()
                csv = self.get_csv()
                zip_file.writestr(f'{self.pandataset.id}_data.csv', csv)
                zip_file.writestr(f'{self.pandataset.id}_metadata.json', package)
                self.logging.append({'SUCCESS': 'Frictionless in memory ZIP created'})
            except Exception as e:
                self.logging.append(
                    {
                        'ERROR': f'Frictionless in memory Zip creation failed: {str(e)}'
                    }
                )

        else:
            self.logging.append({'ERROR': 'Dataset is protected'})
        return in_memory_zip

    def save(self):
        if isinstance(self.file, BytesIO):
            try:
                with open(os.path.join(self.filelocation, str(f'frictionless_pangaea_{str(self.pandataset.id)}.zip')), 'wb') as f:
                    f.write(self.file.getbuffer())
                    f.close()
                    return True
            except Exception as e:
                self.logging.append({'ERROR': f'Could not save Frictionless Zip: {str(e)}'})
        else:
            self.logging.append({'ERROR':'Could not save, Frictionless Zip file is not a BytesIO'})
            return False