/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.fetchers.ddl;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTypeFetcher extends DdlQueryFetcher {

  public CreateTypeFetcher(Persistence persistence, AuthenticationService authenticationService) {
    super(persistence, authenticationService);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment, QueryBuilder builder) {

    List<Map<String, Object>> fieldList = dataFetchingEnvironment.getArgument("fields");
    if (fieldList.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one field");
    }
    List<Column> fields = new ArrayList<>(fieldList.size());
    for (Map<String, Object> key : fieldList) {
      fields.add(Column.create((String) key.get("name"), decodeType(key.get("type"))));
    }
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    UserDefinedType udt =
        ImmutableUserDefinedType.builder()
            .keyspace(keyspaceName)
            .name(dataFetchingEnvironment.getArgument("typeName"))
            .addAllColumns(fields)
            .build();

    Boolean ifNotExists = dataFetchingEnvironment.getArgument("ifNotExists");
    return builder
        .create()
        .type(keyspaceName, udt)
        .ifNotExists(ifNotExists != null && ifNotExists)
        .build();
  }
}