import {Box, Menu, MenuItem, Popover} from '@dagster-io/ui-components';
import {Button, ExternalAnchorButton} from '@dagster-io/ui-components/src/components/Button';
import {Icon} from '@dagster-io/ui-components/src/components/Icon';
import * as React from 'react';

import {CodeLinkProtocolContext} from './CodeLinkProtocol';
import {SourceMetadataEntry} from '../graphql/types';

export const CodeLink = ({codeLinkData}: {codeLinkData: SourceMetadataEntry}) => {
  const [codeLinkProtocol, _] = React.useContext(CodeLinkProtocolContext);

  const sources = codeLinkData.sources;

  const codeSourceDataByKey =
    codeLinkData &&
    codeLinkData.sources.reduce(
      (acc: {[key: string]: {filePath: string; lineNumber: any}}, {key, source}) => {
        acc[key] = source;
        return acc;
      },
      {},
    );

  // use asset_definition as key if it's a key in codeSourceUrlsData object, else use the first key alphabetically
  const defaultKey = codeLinkData
    ? Object.keys(codeSourceDataByKey).includes('asset_definition')
      ? 'asset_definition'
      : Object.keys(codeSourceDataByKey).sort()[0]
    : undefined;

  const defaultCodeSourceData =
    codeSourceDataByKey && defaultKey ? codeSourceDataByKey[defaultKey] : undefined;

  const otherKeys = codeLinkData
    ? sources.map(({key}) => key).filter((key) => key !== defaultKey)
    : [];
  const hasMultipleCodeSources = otherKeys.length > 0;

  const codeLink = codeLinkProtocol.protocol
    .replace('{FILE}', defaultCodeSourceData?.filePath || '')
    .replace('{LINE}', (defaultCodeSourceData?.lineNumber || '0').toString());
  return (
    <Box flex={{alignItems: 'center'}}>
      <ExternalAnchorButton
        icon={<Icon name="open_in_new" />}
        href={codeLink}
        style={
          hasMultipleCodeSources
            ? {
                borderTopRightRadius: 0,
                borderBottomRightRadius: 0,
                borderRight: '0px',
              }
            : {}
        }
      >
        Open in editor
      </ExternalAnchorButton>
      {hasMultipleCodeSources && (
        <Popover
          position="bottom-right"
          content={
            <Menu>
              {otherKeys.map((key) => (
                <MenuItem
                  key={key}
                  text={`Open ${key} in editor`}
                  onClick={() => {
                    const codeSourceData = codeSourceDataByKey[key] as {
                      filePath: string;
                      lineNumber: number;
                    };
                    const codeLink = codeLinkProtocol.protocol
                      .replace('{FILE}', codeSourceData.filePath)
                      .replace('{LINE}', codeSourceData.lineNumber.toString());
                    window.open(codeLink, '_blank');
                  }}
                />
              ))}
            </Menu>
          }
        >
          <Button
            icon={<Icon name="expand_more" />}
            style={{
              minWidth: 'initial',
              borderTopLeftRadius: 0,
              borderBottomLeftRadius: 0,
              marginLeft: '-1px',
            }}
          />
        </Popover>
      )}
    </Box>
  );
};
