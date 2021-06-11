from google.cloud import datacatalog
from google.api_core.exceptions import AlreadyExists


class DataCatalogClient:
    def __init__(self, project, location, credentials=None):
        self.project = project
        self.location = location
        self.client = datacatalog.DataCatalogClient(credentials=credentials)

    def lookup_bq_entry(self, project, dataset, table=None):
        if not table:
            linked_resource = (
                f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}"
            )
        else:
            linked_resource = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
        return self.client.lookup_entry(request={"linked_resource": linked_resource})

    def get_tag_template(self, tag_template, project=None, location=None):
        if not project:
            project = self.project
        if not location:
            location = self.location
        path = self.client.tag_template_path(project, location, tag_template)
        return self.client.get_tag_template(name=path)

    @staticmethod
    def create_tag_from_template_enumtypes(tag_template):
        """
        Generates a Tag containing the first value for each field
        from a Tag Template with only enumTypes.
        """
        tag = datacatalog.Tag()
        tag.template = tag_template.name
        for key in tag_template.fields.keys():
            tag.fields[key] = datacatalog.TagField()
            tag.fields[key].enum_value.display_name = (
                tag_template.fields[key].type_.enum_type.allowed_values[0].display_name
            )
        return tag

    def create_tag(self, parent, tag):
        self.client.create_tag(parent=parent, tag=tag)

    def attach_bq_tag_from_template(
        self, tag_template, project, dataset, table=None
    ):
        """
        Given a Tag Template with only enumTypes, this will tag a BigQuery
        resource with the first allowed value for each of the template's fields.
        """

        bq_entry = self.lookup_bq_entry(project, dataset, table)
        tag_template = self.get_tag_template(tag_template, project=self.project)
        tag = self.create_tag_from_template_enumtypes(tag_template)
        try:
            self.client.create_tag(parent=bq_entry.name, tag=tag)
        except AlreadyExists:
            pass
